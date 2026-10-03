#!/usr/bin/env python3
"""Koyomi: a small job scheduler for a few machines.

One machine is the hub. It keeps all state as plain JSON under ~/.koyomi/
(override with KOYOMI_HOME):

  jobs/<id>.json               job definition + scheduling state
  runs/<id>/<run_id>.json      one record per run (status, exit code, timing)
  runs/<id>/<run_id>.log       combined stdout/stderr of that run
  hosts/<name>.json            registered hosts and their scheduler heartbeat
  alerts/<alert_id>.json       alert emails, pending until sent
  daemon.log                   scheduler events from every host

Every machine has a config.json (its host name, how to reach the hub, email
settings) and runs `koyomi daemon` (launchd on macOS, systemd on Linux). Every
job is pinned to a host. A host's daemon asks the hub for its due runs, runs
them, and streams their output back. On the hub, hub operations are function
calls; other machines run the same operations through `ssh <hub> koyomi _rpc`.
Hosts keep only config.json and a spool of in-progress output.

The hub claims a slot by persisting the advanced next_run *before* a runner
starts, so a slot is never executed twice (at-most-once), and missed slots
collapse into a single catch-up run after sleep or downtime. The hub emails an
alert when a run fails, times out or is interrupted, a slot is skipped, an
always-on host runs a slot late or stops reporting; a machine that cannot
reach the hub emails that itself.
"""

from __future__ import annotations

import argparse
import base64
import codecs
import contextlib
import datetime as dt
import fcntl
import json
import os
import plistlib
import queue
import re
import shlex
import signal
import smtplib
import ssl
import subprocess
import sys
import threading
import time
import traceback
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

VERSION = "0.2.0"
LABEL = "local.koyomi.scheduler"  # launchd agent (macOS)
UNIT = "koyomi.service"  # systemd unit (Linux)
TICK_SECONDS = 5  # scheduler pass interval on every host
SYNC_SECONDS = 2  # runner -> hub output upload and stop-request check
GRACE_SECONDS = 300  # a slot started later than this counts as missed
HOST_LIVE_SECONDS = 30  # a host seen this recently can take a manual run
HOST_DOWN_SECONDS = 180  # unreachable this long -> alert
RPC_TIMEOUT = 60
KEEP_RUNS = 50  # run records kept per job
KEEP_ALERTS = 200  # sent alerts kept
ALERT_RETRY_SECONDS = 60  # first wait after a failed send; doubles per attempt
ALERT_RETRY_MAX = 3600  # longest wait between send attempts
LOG_CHUNK = 1_000_000  # max bytes uploaded per sync
MIN_INTERVAL = 60
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
ACTIVE_STATES = {"queued", "running"}
ALERT_STATES = {"failed", "timeout", "interrupted"}  # also FAILURE_STATES in the UI
SMTP_KEYS = ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD")


class KoyomiError(Exception):
    pass


class HubUnreachable(KoyomiError):
    pass


# ---------------------------------------------------------------- paths & io


def home() -> Path:
    return Path(os.environ.get("KOYOMI_HOME", "~/.koyomi")).expanduser()


def config_path() -> Path:
    return home() / "config.json"


def jobs_dir() -> Path:
    return home() / "jobs"


def job_path(job_id: str) -> Path:
    return jobs_dir() / f"{job_id}.json"


def runs_dir(job_id: str) -> Path:
    return home() / "runs" / job_id


def run_json_path(job_id: str, run_id: str) -> Path:
    return runs_dir(job_id) / f"{run_id}.json"


def run_log_path(job_id: str, run_id: str) -> Path:
    return runs_dir(job_id) / f"{run_id}.log"


def hosts_dir() -> Path:
    return home() / "hosts"


def host_path(name: str) -> Path:
    return hosts_dir() / f"{name}.json"


def alerts_dir() -> Path:
    return home() / "alerts"


def spool_path(run_id: str) -> Path:
    return home() / "spool" / f"{run_id}.log"


def plist_path() -> Path:
    return Path("~/Library/LaunchAgents").expanduser() / f"{LABEL}.plist"


def unit_path() -> Path:
    return Path("/etc/systemd/system") / UNIT


def read_json(path: Path):
    with open(path) as f:
        return json.load(f)


def write_json(path: Path, data, private: bool = False) -> None:
    """Atomic write: temp file + fsync + rename. private=True makes it mode 600."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as f:
        if private:
            os.chmod(tmp, 0o600)
        json.dump(data, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@contextlib.contextmanager
def store_lock():
    """Global lock around every read-modify-write of hub state."""
    home().mkdir(parents=True, exist_ok=True)
    with open(home() / "koyomi.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load_config() -> dict:
    try:
        return read_json(config_path())
    except FileNotFoundError:
        raise KoyomiError(
            f"{config_path()} not found; set this machine up first: koyomi init HOST ..."
        ) from None


def say(message: str) -> None:
    """Host-local process message: goes to the service log (launchd.log / journald)."""
    print(f"{iso(now())} {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- time


def now() -> dt.datetime:
    return dt.datetime.now().astimezone()


def iso(t: dt.datetime | None) -> str | None:
    return t.isoformat(timespec="seconds") if t else None


def parse_iso(s: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(s) if s else None


def zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise KoyomiError(
            f"unknown time zone {name!r} (example: America/Toronto)"
        ) from None


def local_tz_name() -> str:
    """IANA name of this machine's zone, from the /etc/localtime symlink."""
    m = re.search(r"zoneinfo/(.+)$", os.path.realpath("/etc/localtime"))
    if not m:
        raise KoyomiError("cannot tell this machine's time zone; pass --tz")
    return m.group(1)


def parse_duration(text: str) -> int:
    """'90s', '15m', '2h', '1d', '1w', '1h30m' -> seconds."""
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    t = str(text).strip().lower()
    parts = re.findall(r"(\d+)\s*([smhdw])", t)
    if not parts or re.sub(r"(\d+)\s*([smhdw])", "", t).strip():
        raise KoyomiError(
            f"invalid duration {text!r} (examples: 30s, 15m, 2h, 1d, 1h30m)"
        )
    return sum(int(n) * units[u] for n, u in parts)


def parse_when(text: str, tz: ZoneInfo) -> dt.datetime:
    """'HH:MM' (next occurrence) or ISO-ish 'YYYY-MM-DD HH:MM[:SS][+offset]', in tz."""
    v = text.strip()
    if re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", v):
        parts = [int(x) for x in v.split(":")]
        cur = now().astimezone(tz)
        t = cur.replace(
            hour=parts[0],
            minute=parts[1],
            second=parts[2] if len(parts) > 2 else 0,
            microsecond=0,
        )
        return t if t > cur else t + dt.timedelta(days=1)
    try:
        t = dt.datetime.fromisoformat(v)
    except ValueError:
        raise KoyomiError(
            f"invalid time {text!r} (use 'YYYY-MM-DD HH:MM' or 'HH:MM')"
        ) from None
    return t.replace(tzinfo=tz) if t.tzinfo is None else t


def fmt_time(value) -> str:
    t = parse_iso(value) if isinstance(value, str) else value
    return t.astimezone().strftime("%Y-%m-%d %H:%M:%S") if t else "-"


def fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    if s < 86400:
        return f"{s // 3600}h{s % 3600 // 60:02d}m"
    return f"{s // 86400}d{s % 86400 // 3600:02d}h"


def fmt_rel(value) -> str:
    t = parse_iso(value) if isinstance(value, str) else value
    if not t:
        return ""
    delta = (t - now()).total_seconds()
    return f"in {fmt_dur(delta)}" if delta >= 0 else f"{fmt_dur(-delta)} ago"


# ---------------------------------------------------------------- cron

CRON_ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
MONTH_NAMES = {
    m: i + 1
    for i, m in enumerate(
        [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ]
    )
}
DOW_NAMES = {
    d: i for i, d in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])
}


def _cron_field(text: str, lo: int, hi: int, names: dict) -> set[int]:
    def value(tok: str) -> int:
        if tok in names:
            return names[tok]
        if not tok.isdigit():
            raise ValueError(tok)
        return int(tok)

    out: set[int] = set()
    for part in text.lower().split(","):
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            step = int(step_text)
            if step < 1:
                raise ValueError(step_text)
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            a, b = (value(x) for x in part.split("-", 1))
        else:
            a = value(part)
            b = hi if step > 1 else a
        if not (lo <= a <= b <= hi):
            raise ValueError(part)
        out.update(range(a, b + 1, step))
    return out


class Cron:
    """Standard 5-field cron (minute hour day-of-month month day-of-week) in a time zone."""

    def __init__(self, expr: str):
        fields = CRON_ALIASES.get(expr.strip().lower(), expr).split()
        if len(fields) != 5:
            raise KoyomiError(
                f"invalid cron {expr!r}: need 5 fields (minute hour day month weekday)"
            )
        try:
            self.minutes = sorted(_cron_field(fields[0], 0, 59, {}))
            self.hours = sorted(_cron_field(fields[1], 0, 23, {}))
            self.days = _cron_field(fields[2], 1, 31, {})
            self.months = _cron_field(fields[3], 1, 12, MONTH_NAMES)
            self.dows = {d % 7 for d in _cron_field(fields[4], 0, 7, DOW_NAMES)}
        except ValueError as e:
            raise KoyomiError(f"invalid cron {expr!r}: bad value {e}") from None
        # Vixie cron: if both day fields are restricted, a day matches either.
        self.either_day = not fields[2].startswith("*") and not fields[4].startswith(
            "*"
        )

    def day_matches(self, d: dt.date) -> bool:
        if d.month not in self.months:
            return False
        dom, dow = d.day in self.days, d.isoweekday() % 7 in self.dows
        return (dom or dow) if self.either_day else (dom and dow)

    def next_after(self, after: dt.datetime, tz: ZoneInfo) -> dt.datetime:
        start = after.astimezone(tz).replace(
            tzinfo=None, second=0, microsecond=0
        ) + dt.timedelta(minutes=1)
        day = start.date()
        for _ in range(366 * 8):
            if self.day_matches(day):
                for h in self.hours:
                    for m in self.minutes:
                        cand = dt.datetime.combine(day, dt.time(h, m))
                        if cand < start:
                            continue
                        # normalizes a wall time inside a DST gap to the real instant
                        aware = cand.replace(tzinfo=tz)
                        aware = aware.astimezone(dt.timezone.utc).astimezone(tz)
                        if aware > after:
                            return aware
            day += dt.timedelta(days=1)
        raise KoyomiError("cron expression never matches")


# ---------------------------------------------------------------- scheduling


def describe_schedule(job: dict) -> str:
    s = job["schedule"]
    if "cron" in s:
        return f"cron {s['cron']} ({job['timezone']})"
    if "every" in s:
        return f"every {s['every']}"
    return f"once at {fmt_time(s['at'])}"


def compute_next(job: dict, after: dt.datetime) -> dt.datetime | None:
    s = job["schedule"]
    if "cron" in s:
        return Cron(s["cron"]).next_after(after, zone(job["timezone"]))
    if "every" in s:
        secs = parse_duration(s["every"])
        anchor = parse_iso(s["anchor"])
        assert anchor is not None
        if after < anchor:
            return anchor
        k = int((after - anchor).total_seconds() // secs) + 1
        return anchor + dt.timedelta(seconds=k * secs)
    at = parse_iso(s["at"])
    assert at is not None
    return at if at > after else None


def refresh_next_run(job: dict) -> None:
    if not job["enabled"]:
        job["next_run"] = None
        return
    job["next_run"] = iso(compute_next(job, now()))
    if job["next_run"] is None and "at" in job["schedule"]:
        raise KoyomiError(f"time {fmt_time(job['schedule']['at'])} is in the past")


def boot_time() -> dt.datetime:
    global _BOOT_TIME
    if _BOOT_TIME is None:
        if sys.platform == "darwin":
            out = subprocess.run(
                ["sysctl", "-n", "kern.boottime"], capture_output=True, text=True
            ).stdout
            m = re.search(r"sec = (\d+)", out)
        else:
            m = re.search(
                r"^btime (\d+)$", Path("/proc/stat").read_text(), re.MULTILINE
            )
        if not m:
            raise KoyomiError("cannot read the system boot time")
        _BOOT_TIME = dt.datetime.fromtimestamp(int(m.group(1))).astimezone()
    return _BOOT_TIME


_BOOT_TIME = None


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def tail_bytes(path: Path, n: int = 2000) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - n))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


# ---------------------------------------------------------------- hub: state
#
# Everything in this section runs on the hub, on the hub's ~/.koyomi.


def log_event(message: str) -> None:
    path = home() / "daemon.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 5_000_000:
        os.replace(path, path.with_name("daemon.log.1"))
    with open(path, "a") as f:
        f.write(f"{iso(now())} {message}\n")


def load_job(job_id: str) -> dict:
    try:
        return read_json(job_path(job_id))
    except FileNotFoundError:
        raise KoyomiError(f"no such job: {job_id}") from None


def load_jobs() -> list[dict]:
    jobs = []
    for path in sorted(jobs_dir().glob("*.json")):
        try:
            jobs.append(read_json(path))
        except (OSError, ValueError) as e:
            log_event(f"warning: cannot read {path}: {e}")
    return jobs


def save_job(job: dict) -> None:
    write_json(job_path(job["id"]), job)


def load_runs(job_id: str) -> list[dict]:
    runs = []
    for path in sorted(runs_dir(job_id).glob("*.json")):
        with contextlib.suppress(OSError, ValueError):
            runs.append(read_json(path))
    return runs


def load_run(job_id: str, run_id: str) -> dict:
    try:
        return read_json(run_json_path(job_id, run_id))
    except FileNotFoundError:
        raise KoyomiError(f"no such run: {job_id} {run_id}") from None


def load_host(name: str) -> dict:
    try:
        return read_json(host_path(name))
    except FileNotFoundError:
        known = ", ".join(h["name"] for h in load_hosts()) or "none"
        raise KoyomiError(
            f"unknown host {name!r} (registered: {known}; add one with koyomi init)"
        ) from None


def load_hosts() -> list[dict]:
    return [read_json(p) for p in sorted(hosts_dir().glob("*.json"))]


def host_live(host: dict, cur: dt.datetime) -> bool:
    seen = parse_iso(host.get("last_seen"))
    return bool(seen) and (cur - seen).total_seconds() <= HOST_LIVE_SECONDS


def run_summary(rec: dict) -> dict:
    keys = (
        "run_id",
        "trigger",
        "status",
        "exit_code",
        "started_at",
        "finished_at",
        "error",
    )
    return {k: rec.get(k) for k in keys}


def new_run_record(
    job: dict, trigger: str, cur: dt.datetime, scheduled_for=None
) -> dict:
    stamp = cur.astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return {
        "run_id": f"{stamp}-{os.urandom(2).hex()}",
        "job_id": job["id"],
        "host": job["host"],
        "trigger": trigger,
        "status": "running",
        "scheduled_for": iso(scheduled_for),
        "created_at": iso(cur),
        "started_at": iso(cur),
        "finished_at": None,
        "duration_seconds": None,
        "exit_code": None,
        "error": None,
        "pid": None,
        "stop_requested_at": None,
        "command": job["command"],
        "cwd": job["cwd"],
        "env": job["env"],
        "timeout": job["timeout"],
    }


def claim_run(
    job: dict, trigger: str, cur: dt.datetime, scheduled_for=None, queued=False
) -> dict:
    """Create the run record and mark the job busy (caller holds the lock and saves)."""
    rec = new_run_record(job, trigger, cur, scheduled_for)
    if queued:
        rec.update(status="queued", started_at=None)
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    job["running"] = {
        "run_id": rec["run_id"],
        "status": rec["status"],
        "pid": None,
        "started_at": rec["started_at"],
        "trigger": trigger,
    }
    return rec


def raise_alert(subject: str, body: str) -> None:
    cur = now()
    alert_id = cur.astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    alert_id += "-" + os.urandom(2).hex()
    write_json(
        alerts_dir() / f"{alert_id}.json",
        {
            "id": alert_id,
            "subject": subject,
            "body": body,
            "created_at": iso(cur),
            "sent_at": None,
            "attempts": 0,
            "last_attempt_at": None,
            "last_error": None,
        },
    )
    log_event(f"alert: {subject}")


def run_alert(rec: dict) -> None:
    lines = [
        f"Job:       {rec['job_id']}",
        f"Host:      {rec['host']}",
        f"Status:    {rec['status']}",
        f"Error:     {rec.get('error') or '-'}",
        f"Run:       {rec['run_id']} ({rec['trigger']})",
    ]
    if rec.get("scheduled_for"):
        lines.append(f"Slot:      {rec['scheduled_for']}")
    lines += [
        f"Started:   {rec.get('started_at') or '-'}",
        f"Duration:  {fmt_dur(rec.get('duration_seconds'))}",
        f"Command:   {rec['command']}",
        f"Directory: {rec['cwd']}",
        "",
        "Last output:",
        rec.get("output_tail") or "(none)",
        "",
        f"Full log: koyomi logs {rec['job_id']} --run {rec['run_id']} -n 0",
        "",
        "No further alerts for this job until it succeeds again.",
    ]
    raise_alert(
        f"Koyomi: {rec['job_id']} {rec['status']} on {rec['host']}", "\n".join(lines)
    )


def record_skip(
    job: dict, reason: str, due: dt.datetime, cur: dt.datetime, alert: bool
) -> None:
    rec = new_run_record(job, "schedule", cur, due)
    rec.update(status="skipped", finished_at=iso(cur), error=reason)
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    if not job.get("running"):
        job["last_run"] = run_summary(rec)
    log_event(f"{job['id']}: skipped slot {iso(due)}: {reason}")
    if alert and not job.get("alerted"):
        job["alerted"] = True
        raise_alert(
            f"Koyomi: {job['id']} skipped a slot on {job['host']}",
            f"Job {job['id']} on {job['host']} skipped its {iso(due)} slot: {reason}.\n"
            "No further alerts for this job until it succeeds again.",
        )


def prune_runs(job_id: str) -> None:
    records = sorted(runs_dir(job_id).glob("*.json"))
    for path in records[:-KEEP_RUNS]:
        with contextlib.suppress(OSError, ValueError):
            if read_json(path).get("status") in ACTIVE_STATES:
                continue
            path.unlink()
            path.with_suffix(".log").unlink(missing_ok=True)


def close_run(job: dict, rec: dict) -> None:
    """Persist a finished run on its job (caller holds the lock)."""
    if rec["status"] != "success":
        rec["output_tail"] = tail_bytes(run_log_path(job["id"], rec["run_id"]))
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    if (job.get("running") or {}).get("run_id") == rec["run_id"]:
        job["running"] = None
    job["last_run"] = run_summary(rec)
    alert = rec["status"] in ALERT_STATES and not job.get("alerted")
    if rec["status"] == "success":
        job["alerted"] = False
    elif alert:
        job["alerted"] = True
    save_job(job)
    error = f" ({rec['error']})" if rec.get("error") else ""
    log_event(f"{job['id']}: run {rec['run_id']} {rec['status']}{error}")
    if alert:
        run_alert(rec)
    prune_runs(job["id"])


# ---------------------------------------------------------------- hub: operations
#
# The hub's whole API. Arguments and results are plain JSON values.


def op_register_host(name: str, always_on: bool, platform: str) -> dict:
    if not ID_RE.match(name):
        raise KoyomiError("host name must be letters, digits, '.', '_' or '-'")
    with store_lock():
        path = host_path(name)
        host = (
            read_json(path)
            if path.exists()
            else {"name": name, "registered_at": iso(now()), "last_seen": None}
        )
        host.update(always_on=always_on, platform=platform)
        write_json(path, host)
    log_event(f"[{name}] host registered (always on: {always_on})")
    return host


def op_tick(host: str, daemon: dict) -> dict:
    """One scheduler pass for a host: heartbeat, then claim its due and queued runs.

    Returns the runs the host must start, and every active run on it so the host
    can check that their runner processes are still alive.
    """
    cur = now()
    start: list[dict] = []
    with store_lock():
        h = load_host(host)
        if (h.get("daemon") or {}).get("pid") != daemon["pid"]:
            log_event(
                f"[{host}] scheduler started (pid {daemon['pid']}, version {daemon['version']})"
            )
        if h.get("down_alerted"):
            log_event(f"[{host}] host reporting again")
        h.update(last_seen=iso(cur), daemon=daemon, down_alerted=False)
        write_json(host_path(host), h)
        for job in load_jobs():
            if job["host"] != host:
                continue
            r = job.get("running")
            if r and r["status"] == "queued":
                rec = load_run(job["id"], r["run_id"])
                rec.update(status="running", started_at=iso(cur))
                write_json(run_json_path(job["id"], rec["run_id"]), rec)
                r.update(status="running", started_at=rec["started_at"])
                save_job(job)
                log_event(f"{job['id']}: starting manual run {rec['run_id']}")
                start.append(rec)
            due = parse_iso(job.get("next_run"))
            if not (job.get("enabled") and due and due <= cur):
                continue
            # Claim the slot first: persist the advanced next_run before running anything.
            job["next_run"] = iso(compute_next(job, cur))
            late = (cur - due).total_seconds()
            if job.get("running"):
                record_skip(job, "previous run still in progress", due, cur, True)
            elif late > GRACE_SECONDS and job["catchup"] == "skip":
                reason = f"missed by {fmt_dur(late)} (catchup=skip)"
                record_skip(job, reason, due, cur, h["always_on"])
            else:
                rec = claim_run(job, "schedule", cur, due)
                if late > GRACE_SECONDS:
                    log_event(
                        f"{job['id']}: catch-up run for missed slot {iso(due)} "
                        f"({fmt_dur(late)} late)"
                    )
                    if h["always_on"]:
                        raise_alert(
                            f"Koyomi: {job['id']} ran late on {host}",
                            f"Job {job['id']} on {host} missed its {iso(due)} slot and "
                            f"started {fmt_dur(late)} late, as a catch-up run "
                            f"({rec['run_id']}). The host or its scheduler was down, "
                            "or could not reach the hub.",
                        )
                log_event(f"{job['id']}: starting run {rec['run_id']}")
                start.append(rec)
            save_job(job)
        running = []
        for job in load_jobs():
            r = job.get("running")
            if job["host"] == host and r and r["status"] == "running":
                log = run_log_path(job["id"], r["run_id"])
                size = log.stat().st_size if log.exists() else 0
                running.append({**r, "job_id": job["id"], "log_size": size})
    return {"start": start, "running": running}


def op_mark_interrupted(host: str, runs: list[dict]) -> None:
    """The host found these runners gone (reboot, crash, kill)."""
    with store_lock():
        for r in runs:
            if not job_path(r["job_id"]).exists():
                continue
            job = load_job(r["job_id"])
            rec = load_run(r["job_id"], r["run_id"])
            if rec["status"] != "running":
                continue
            rec.update(status="interrupted", finished_at=iso(now()), error=r["reason"])
            close_run(job, rec)


def op_run_started(job_id: str, run_id: str, pid: int) -> dict:
    """A runner reports in; returns the run record it must execute."""
    with store_lock():
        job, rec = load_job(job_id), load_run(job_id, run_id)
        if rec["status"] != "running":
            raise KoyomiError(f"run {run_id} is {rec['status']}, not running")
        rec["pid"] = pid
        write_json(run_json_path(job_id, run_id), rec)
        if (job.get("running") or {}).get("run_id") == run_id:
            job["running"]["pid"] = pid
            save_job(job)
    return rec


def op_run_progress(job_id: str, run_id: str, offset: int, data: str) -> dict:
    """Append output at a byte offset (idempotent on retry); report a stop request."""
    try:
        rec = load_run(job_id, run_id)
    except KoyomiError:
        return {"size": offset, "stop": True}  # the job was deleted
    path = run_log_path(job_id, run_id)
    size = path.stat().st_size if path.exists() else 0
    if offset > size:
        raise KoyomiError(
            f"log gap for {run_id}: hub has {size} bytes, got offset {offset}"
        )
    chunk = base64.b64decode(data)[size - offset :]
    if chunk:
        with open(path, "ab") as f:
            f.write(chunk)
        size += len(chunk)
    return {"size": size, "stop": bool(rec.get("stop_requested_at"))}


def op_run_finished(job_id: str, run_id: str, result: dict) -> None:
    with store_lock():
        if not job_path(job_id).exists():
            log_event(f"{job_id}: run {run_id} {result['status']} (job was deleted)")
            return
        job, rec = load_job(job_id), load_run(job_id, run_id)
        if rec["status"] != "running":
            return  # already closed: a retried report
        rec.update(result)
        close_run(job, rec)


def op_add_job(job: dict) -> dict:
    if not ID_RE.match(job["id"]):
        raise KoyomiError(
            "job id must be letters, digits, '.', '_' or '-' (max 64 chars)"
        )
    zone(job["timezone"])
    if job["timeout"]:
        parse_duration(job["timeout"])
    ts = iso(now())
    job = {
        **job,
        "created_at": ts,
        "updated_at": ts,
        "next_run": None,
        "last_run": None,
        "alerted": False,
        "running": None,
    }
    refresh_next_run(job)
    with store_lock():
        host = load_host(job["host"])
        if job_path(job["id"]).exists():
            raise KoyomiError(f"job already exists: {job['id']} (use: koyomi update)")
        save_job(job)
    return {"job": job, "host_live": host_live(host, now())}


def op_update_job(job_id: str, changes: dict, unset_env: list[str]) -> dict:
    with store_lock():
        job = load_job(job_id)
        if "host" in changes:
            load_host(changes["host"])
            if job.get("running"):
                raise KoyomiError(f"{job_id} is running; move it after the run ends")
        if "timezone" in changes:
            zone(changes["timezone"])
        if changes.get("timeout"):
            parse_duration(changes["timeout"])
        env = {**job["env"], **changes.pop("env", {})}
        for key in unset_env:
            env.pop(key, None)
        job.update(changes, env=env, updated_at=iso(now()))
        if "schedule" in changes or "timezone" in changes:
            refresh_next_run(job)
        save_job(job)
    return job


def op_set_enabled(job_id: str, enabled: bool) -> dict:
    """Change a job's scheduling state without affecting an active run."""
    with store_lock():
        job = load_job(job_id)
        job["enabled"] = enabled
        refresh_next_run(job)  # re-enabling never back-fills the disabled period
        job["updated_at"] = iso(now())
        save_job(job)
    return job


def op_delete_job(job_id: str, keep_logs: bool) -> dict:
    import shutil

    with store_lock():
        job = load_job(job_id)
        job_path(job_id).unlink()
        if not keep_logs:
            shutil.rmtree(runs_dir(job_id), ignore_errors=True)
    log_event(f"{job_id}: deleted")
    return job


def op_queue_run(job_id: str) -> dict:
    """Queue a manual run; the job's host starts it on its next tick."""
    cur = now()
    with store_lock():
        job = load_job(job_id)
        if job.get("running"):
            r = job["running"]
            raise KoyomiError(f"{job_id} is already {r['status']} (run {r['run_id']})")
        host = load_host(job["host"])
        if not host_live(host, cur):
            raise KoyomiError(
                f"host {job['host']} was last seen {fmt_rel(host['last_seen']) or 'never'}; "
                "its scheduler is not running, or the machine is asleep or offline"
            )
        rec = claim_run(job, "manual", cur, queued=True)
        save_job(job)
    log_event(f"{job_id}: manual run {rec['run_id']} queued for {job['host']}")
    return rec


def op_request_stop(job_id: str) -> str:
    """Ask the runner to stop; it writes the final record. A queued run stops at once."""
    with store_lock():
        job = load_job(job_id)
        r = job.get("running")
        if not r:
            raise KoyomiError(f"{job_id} is not running")
        rec = load_run(job_id, r["run_id"])
        if rec["status"] == "queued":
            rec.update(
                status="stopped",
                finished_at=iso(now()),
                error="stopped before it started",
            )
            close_run(job, rec)
        else:
            rec["stop_requested_at"] = iso(now())
            write_json(run_json_path(job_id, rec["run_id"]), rec)
    log_event(f"{job_id}: stop requested for run {r['run_id']}")
    return r["run_id"]


def op_job(job_id: str) -> dict:
    return load_job(job_id)


def op_jobs() -> list[dict]:
    return load_jobs()


def op_snapshot() -> dict:
    alerts = [read_json(p) for p in sorted(alerts_dir().glob("*.json"))]
    return {
        "hosts": load_hosts(),
        "jobs": load_jobs(),
        "unsent_alerts": [a for a in alerts if not a["sent_at"]],
    }


def op_runs(job_id: str | None, failed: bool, limit: int) -> list[dict]:
    ids = [job_id] if job_id else [j["id"] for j in load_jobs()]
    if job_id:
        load_job(job_id)
    runs = sorted((r for i in ids for r in load_runs(i)), key=lambda r: r["run_id"])
    if failed:
        runs = [r for r in runs if r["status"] in ALERT_STATES | {"skipped"}]
    return runs[-limit:] if limit else runs


def op_log(
    job_id: str, run_id: str | None, offset: int | None = None, lines: int | None = None
) -> dict:
    """A run's output: bytes from offset, or the last N lines (0 = all), base64."""
    load_job(job_id)
    if run_id is None:
        runs = [
            r for r in load_runs(job_id) if r["status"] not in ("skipped", "queued")
        ]
        if not runs:
            raise KoyomiError(f"{job_id} has no runs yet")
        run_id = max(runs, key=lambda r: r["run_id"])["run_id"]
    rec = load_run(job_id, run_id)
    path = run_log_path(job_id, run_id)
    raw = path.read_bytes() if path.exists() else b""
    if offset is not None:
        data = raw[offset:]
    else:
        data = b"\n".join(raw.splitlines()[-lines:] if lines else raw.splitlines())
    return {"run": rec, "data": base64.b64encode(data).decode(), "size": len(raw)}


def op_daemon_log(lines: int) -> str:
    path = home() / "daemon.log"
    text = path.read_text(errors="replace") if path.exists() else ""
    return "\n".join(text.splitlines()[-lines:] if lines else text.splitlines())


HUB_OPS = {
    "register_host": op_register_host,
    "tick": op_tick,
    "mark_interrupted": op_mark_interrupted,
    "run_started": op_run_started,
    "run_progress": op_run_progress,
    "run_finished": op_run_finished,
    "add_job": op_add_job,
    "update_job": op_update_job,
    "set_enabled": op_set_enabled,
    "delete_job": op_delete_job,
    "queue_run": op_queue_run,
    "request_stop": op_request_stop,
    "job": op_job,
    "jobs": op_jobs,
    "snapshot": op_snapshot,
    "runs": op_runs,
    "log": op_log,
    "daemon_log": op_daemon_log,
}


# ---------------------------------------------------------------- hub: access


def hub(op: str, **args):
    return call_hub(load_config(), op, args)


def call_hub(cfg: dict, op: str, args: dict):
    """Run a hub operation: in-process on the hub, else through the configured command."""
    if cfg["hub"] == "local":
        return json.loads(json.dumps(HUB_OPS[op](**args)))
    command = cfg["hub"]["command"]
    request = json.dumps({"version": VERSION, "op": op, "args": args})
    try:
        p = subprocess.run(
            command, input=request, capture_output=True, text=True, timeout=RPC_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        raise HubUnreachable(f"hub did not answer within {RPC_TIMEOUT}s") from None
    except OSError as e:
        raise HubUnreachable(f"cannot run {command[0]}: {e}") from None
    if command[0] == "ssh" and p.returncode == 255:
        raise HubUnreachable(f"cannot reach the hub: {p.stderr.strip()}")
    try:
        reply = json.loads(p.stdout)
    except ValueError:
        detail = (p.stderr or p.stdout).strip()
        raise KoyomiError(
            f"no reply from the hub (exit {p.returncode}): {detail}"
        ) from None
    if not reply["ok"]:
        raise KoyomiError(reply["error"])
    return reply["result"]


def rpc_main() -> int:
    """Serve one hub operation: JSON request on stdin, JSON reply on stdout."""
    try:
        request = json.load(sys.stdin)
        if request["version"] != VERSION:
            raise KoyomiError(
                f"version mismatch: caller has koyomi {request['version']}, "
                f"hub has {VERSION}; reinstall koyomi on both machines"
            )
        if load_config()["hub"] != "local":
            raise KoyomiError("this machine is not the hub")
        if request["op"] not in HUB_OPS:
            raise KoyomiError(f"unknown hub operation {request['op']!r}")
        reply = {"ok": True, "result": HUB_OPS[request["op"]](**request["args"])}
    except KoyomiError as e:
        reply = {"ok": False, "error": str(e)}
    except Exception as e:
        log_event("rpc error:\n" + traceback.format_exc())
        reply = {
            "ok": False,
            "error": f"hub error: {e!r} (traceback in the hub's daemon.log)",
        }
    json.dump(reply, sys.stdout)
    return 0


def ssh_hub_command(destination: str, koyomi: str) -> list[str]:
    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        "-o", "ServerAliveInterval=10",
        "-o", "ServerAliveCountMax=3",
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={home()}/ssh-%C",
        "-o", "ControlPersist=10m",
        destination,
        koyomi,
        "_rpc",
    ]  # fmt: skip


# ---------------------------------------------------------------- email


def send_email(cfg: dict, subject: str, body: str) -> None:
    email = cfg["email"]
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = email["from"], email["to"], subject
    msg.set_content(body)
    try:
        with smtplib.SMTP(email["smtp_host"], email["smtp_port"], timeout=30) as smtp:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(email["smtp_user"], email["smtp_password"])
            smtp.send_message(msg)
    except (smtplib.SMTPException, OSError) as e:
        raise KoyomiError(f"sending email failed: {e}") from None


def send_pending_alerts(cfg: dict, cur: dt.datetime) -> None:
    """Hub: email unsent alerts. A failure is retried with backoff and logged once."""
    paths = sorted(alerts_dir().glob("*.json"))
    for path in paths:
        alert = read_json(path)
        if alert["sent_at"]:
            continue
        last = parse_iso(alert["last_attempt_at"])
        backoff = min(ALERT_RETRY_SECONDS * 2 ** (alert["attempts"] - 1), ALERT_RETRY_MAX)
        if last and (cur - last).total_seconds() < backoff:
            return  # the mail server is the problem; wait before trying again
        alert.update(attempts=alert["attempts"] + 1, last_attempt_at=iso(cur))
        try:
            send_email(cfg, alert["subject"], alert["body"])
        except KoyomiError as e:
            if alert["attempts"] == 1:
                log_event(f"alert {alert['id']}: {e}; retrying with backoff")
            alert["last_error"] = str(e)
            write_json(path, alert)
            return  # the mail server is the problem; don't hammer it
        alert["sent_at"] = iso(cur)
        write_json(path, alert)
        if alert["attempts"] > 1:
            log_event(f"alert {alert['id']}: sent after {alert['attempts']} attempts")
    sent = [p for p in paths if read_json(p)["sent_at"]]
    for path in sent[:-KEEP_ALERTS]:
        path.unlink()


def check_hosts_down(cfg: dict, cur: dt.datetime) -> None:
    """Hub: alert once when another always-on host stops reporting."""
    with store_lock():
        for h in load_hosts():
            if not h["always_on"] or h["name"] == cfg["host"] or h.get("down_alerted"):
                continue
            seen = parse_iso(h["last_seen"] or h["registered_at"])
            assert seen is not None
            if (cur - seen).total_seconds() > HOST_DOWN_SECONDS:
                raise_alert(
                    f"Koyomi: host {h['name']} is down",
                    f"Always-on host {h['name']} last reported {fmt_time(seen)} "
                    f"({fmt_rel(seen)}). Its jobs are not running. Check the "
                    f"machine and `systemctl status {UNIT}` there.",
                )
                h["down_alerted"] = True
                write_json(host_path(h["name"]), h)


# ---------------------------------------------------------------- execution
#
# The runner (`koyomi _exec JOB RUN`) runs on the job's host. It writes output
# to a local spool file, which it uploads to the hub every few seconds, so a
# hub outage never loses output or blocks the command.


def kill_group(proc: subprocess.Popen) -> None:
    for sig, wait in ((signal.SIGTERM, 10), (signal.SIGKILL, None)):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, sig)
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


# Each output line starts with the UTC time its first byte arrived and a space.
LOG_STAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z) ")


class StampedLog:
    """Writes command output to the spool, stamping each line as it begins."""

    def __init__(self, f):
        self.f, self.line_start = f, True

    def write(self, data: bytes) -> None:
        out, start = bytearray(), 0
        while start < len(data):
            end = data.find(b"\n", start)
            end = len(data) if end < 0 else end + 1
            if self.line_start:
                stamp = dt.datetime.now(dt.timezone.utc).isoformat(
                    timespec="milliseconds"
                )
                out += stamp.replace("+00:00", "Z ").encode()
            out += data[start:end]
            self.line_start = data[end - 1 : end] == b"\n"
            start = end
        self.f.write(out)

    def pump(self, fd: int) -> None:
        """Copy a pipe into the log until every writer has closed it."""
        with contextlib.suppress(ValueError, OSError):  # ValueError: log closed
            while chunk := os.read(fd, 65536):
                self.write(chunk)


def split_log_line(line: str) -> tuple[str, str]:
    """A log line as (local HH:MM:SS, text); the time is "" for unstamped lines."""
    m = LOG_STAMP.match(line)
    if not m:
        return "", line
    t = dt.datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
    return t.astimezone().strftime("%H:%M:%S"), line[m.end() :]


def render_log_line(line: str) -> str:
    stamp, text = split_log_line(line)
    return f"{stamp}  {text}" if stamp else text


def upload_output(job_id: str, run_id: str, offset: int) -> tuple[int, bool]:
    """Send spooled output from offset; returns (hub's log size, stop requested)."""
    with open(spool_path(run_id), "rb") as f:
        f.seek(offset)
        data = f.read(LOG_CHUNK)
    reply = hub(
        "run_progress",
        job_id=job_id,
        run_id=run_id,
        offset=offset,
        data=base64.b64encode(data).decode(),
    )
    return reply["size"], reply["stop"]


def upload_all(job_id: str, run_id: str, offset: int) -> int:
    size = spool_path(run_id).stat().st_size
    while offset < size:
        offset, _ = upload_output(job_id, run_id, offset)
    return offset


def execute(job_id: str, run_id: str) -> int:
    """Run a claimed job, stream its output to the hub, report the result."""
    rec = hub("run_started", job_id=job_id, run_id=run_id, pid=os.getpid())
    spool = spool_path(run_id)
    spool.parent.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        **rec["env"],
        "KOYOMI_JOB": job_id,
        "KOYOMI_RUN_ID": run_id,
        "KOYOMI_HOST": rec["host"],
    }
    timeout = parse_duration(rec["timeout"]) if rec["timeout"] else None
    status, exit_code, error = "failed", None, None
    started, offset, hub_error = time.time(), 0, None
    with open(spool, "ab") as spool_file:
        log = StampedLog(spool_file)
        try:
            proc = subprocess.Popen(
                ["/bin/sh", "-c", rec["command"]],
                cwd=rec["cwd"],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as e:
            error = f"could not start command: {e}"
            log.write(f"koyomi: {error}\n".encode())
        else:
            assert proc.stdout is not None
            pump = threading.Thread(
                target=log.pump, args=(proc.stdout.fileno(),), daemon=True
            )
            pump.start()
            next_sync = time.time() + SYNC_SECONDS
            try:
                while True:
                    try:
                        exit_code = proc.wait(timeout=0.2)
                    except subprocess.TimeoutExpired:
                        pass
                    else:
                        if exit_code == 0:
                            status = "success"
                        elif exit_code < 0:
                            error = f"killed by signal {-exit_code}"
                        else:
                            error = f"exit code {exit_code}"
                        break
                    if timeout and time.time() - started > timeout:
                        kill_group(proc)
                        status, exit_code = "timeout", proc.returncode
                        error = f"timed out after {rec['timeout']}"
                        break
                    if time.time() < next_sync:
                        continue
                    next_sync = time.time() + SYNC_SECONDS
                    spool_file.flush()
                    try:
                        offset, stop = upload_output(job_id, run_id, offset)
                    except HubUnreachable as e:
                        if hub_error is None:
                            say(
                                f"run {run_id}: {e}; output stays spooled until the hub is back"
                            )
                        hub_error = e
                        continue
                    hub_error = None
                    if stop:
                        kill_group(proc)
                        status, exit_code = "stopped", proc.returncode
                        error = "stopped on request"
                        break
            except BaseException:
                kill_group(proc)
                raise
            # Output a background child still writes after the command exits is
            # not waited for.
            pump.join(timeout=2)
    result = {
        "status": status,
        "exit_code": exit_code,
        "error": error,
        "finished_at": iso(now()),
        "duration_seconds": round(time.time() - started, 3),
    }
    # The result must reach the hub: keep retrying through a hub outage.
    while True:
        try:
            upload_all(job_id, run_id, offset)
            hub("run_finished", job_id=job_id, run_id=run_id, result=result)
            break
        except HubUnreachable as e:
            say(f"run {run_id} {status}: cannot report it yet: {e}")
            time.sleep(15)
    spool.unlink()
    if status == "success":
        return 0
    return exit_code if exit_code and exit_code > 0 else 1


# ---------------------------------------------------------------- daemon


def spawn_runner(job_id: str, run_id: str) -> subprocess.Popen:
    # stderr is inherited: runner problems land in the service log
    return subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "_exec", job_id, run_id],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        start_new_session=True,
    )


def runner_gone(r: dict, cur: dt.datetime) -> str | None:
    """Why an active run's runner is gone, or None while it is alive."""
    started = parse_iso(r["started_at"])
    assert started is not None
    if r["pid"] is None:
        if (cur - started).total_seconds() < 60:
            return None  # just claimed, runner starting
        return "runner never started (it could not reach the hub, or was killed)"
    if pid_alive(r["pid"]) and started >= boot_time():
        return None
    return "runner disappeared (reboot, crash or kill) before finishing"


def host_tick(cfg: dict, info: dict, cur: dt.datetime) -> list[subprocess.Popen]:
    """One scheduler pass on this host. Returns the runners it started."""
    res = call_hub(cfg, "tick", {"host": cfg["host"], "daemon": info})
    dead = []
    for r in res["running"]:
        reason = runner_gone(r, cur)
        if reason is None:
            continue
        spool = spool_path(r["run_id"])
        if spool.exists():  # salvage output the runner had not uploaded
            upload_all(r["job_id"], r["run_id"], r["log_size"])
            spool.unlink()
        dead.append({"job_id": r["job_id"], "run_id": r["run_id"], "reason": reason})
    if dead:
        call_hub(cfg, "mark_interrupted", {"host": cfg["host"], "runs": dead})
    launched = []
    for rec in res["start"]:
        try:
            launched.append(spawn_runner(rec["job_id"], rec["run_id"]))
        except OSError as e:
            result = {
                "status": "failed",
                "exit_code": None,
                "error": f"could not spawn runner: {e}",
                "finished_at": iso(now()),
                "duration_seconds": 0,
            }
            call_hub(
                cfg,
                "run_finished",
                {"job_id": rec["job_id"], "run_id": rec["run_id"], "result": result},
            )
    return launched


class HubWatch:
    """On a non-hub machine: email once when the hub has been unreachable too long."""

    def __init__(self):
        self.since: dt.datetime | None = None
        self.error = ""
        self.alerted = False

    def reachable(self) -> None:
        if self.since:
            say(f"hub reachable again (unreachable since {iso(self.since)})")
        self.since, self.alerted = None, False

    def unreachable(self, cfg: dict, error: Exception, cur: dt.datetime) -> None:
        if self.since is None:
            self.since = cur
            say(f"cannot reach the hub: {error}")
        self.error = str(error)
        if self.alerted or (cur - self.since).total_seconds() <= HOST_DOWN_SECONDS:
            return
        try:
            send_email(
                cfg,
                f"Koyomi: hub unreachable from {cfg['host']}",
                f"{cfg['host']} has not reached the Koyomi hub since {fmt_time(self.since)} "
                f"({fmt_rel(self.since)}).\n\nLast error: {self.error}\n\n"
                "No job is starting anywhere while the hub is down, and jobs on "
                f"{cfg['host']} wait for it. Missed slots run once when it is back.",
            )
            self.alerted = True
            say("emailed a hub-unreachable alert")
        except KoyomiError as e:
            say(f"hub-unreachable alert not sent yet: {e}")


def daemon_main() -> int:
    cfg = load_config()
    home().mkdir(parents=True, exist_ok=True)
    lock = open(home() / "daemon.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("koyomi: scheduler already running", file=sys.stderr)
        return 1
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    signal.signal(signal.SIGINT, lambda *_: stop.append(1))
    started = now()
    info = {
        "pid": os.getpid(),
        "version": VERSION,
        "started_at": iso(started),
        "platform": sys.platform,
    }
    is_hub = cfg["hub"] == "local"
    say(f"scheduler started on {cfg['host']} (pid {os.getpid()}, version {VERSION})")
    runners: list[subprocess.Popen] = []
    watch = HubWatch()
    last, last_ok = started, None
    while not stop:
        cur = now()
        gap = (cur - last).total_seconds()
        if gap > TICK_SECONDS + 60:
            say(f"clock jumped {fmt_dur(gap)} since last tick (sleep/suspend)")
            watch.since = None  # an outage only counts while we are awake
        last = cur
        runners = [p for p in runners if p.poll() is None]  # reap finished runners
        try:
            runners += host_tick(cfg, info, cur)
            last_ok = cur
            if is_hub:
                send_pending_alerts(cfg, cur)
                check_hosts_down(cfg, cur)
            else:
                watch.reachable()
        except HubUnreachable as e:
            watch.unreachable(cfg, e, cur)
        except Exception:
            say("tick error:\n" + traceback.format_exc())
        with contextlib.suppress(OSError):
            write_json(
                home() / "daemon.json",
                {
                    **info,
                    "last_tick": iso(cur),
                    "last_hub_contact": iso(last_ok),
                    "hub_error": watch.error if watch.since else None,
                },
            )
        wake = cur + dt.timedelta(seconds=TICK_SECONDS)
        while not stop and now() < wake:
            time.sleep(0.5)
    say("scheduler stopped")
    return 0


def daemon_state() -> tuple[bool, dict | None]:
    try:
        info = read_json(home() / "daemon.json")
    except (OSError, ValueError):
        return False, None
    return pid_alive(info["pid"]), info


def wait_for_daemon(previous_pid=None, seconds: float = 10) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        alive, info = daemon_state()
        if alive and info and info["pid"] != previous_pid:
            return
        time.sleep(0.2)
    raise KoyomiError("the scheduler did not come up; check the service log")


# ---------------------------------------------------------------- service


def service_env() -> dict:
    """Environment baked into the service: this shell's PATH, so jobs find the same tools."""
    return {
        "PATH": ":".join(
            dict.fromkeys(os.environ.get("PATH", "/usr/bin:/bin").split(":"))
        ),
        "HOME": str(Path.home()),
        "KOYOMI_HOME": str(home()),
    }


def daemon_argv() -> list[str]:
    return [sys.executable, str(Path(__file__).resolve()), "daemon"]


def launchctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["launchctl", *args], capture_output=True, text=True)


def systemctl(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    r = subprocess.run(["systemctl", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise KoyomiError(f"systemctl {' '.join(args)} failed: {r.stderr.strip()}")
    return r


def service_loaded() -> bool:
    if sys.platform == "darwin":
        return launchctl("print", f"gui/{os.getuid()}/{LABEL}").returncode == 0
    return systemctl("is-active", "--quiet", UNIT, check=False).returncode == 0


def service_log_hint() -> str:
    if sys.platform == "darwin":
        return str(home() / "launchd.log")
    return f"journalctl -u {UNIT}"


def cmd_service(args) -> int:
    if sys.platform == "darwin":
        launchd_service(args.action)
    elif sys.platform.startswith("linux"):
        systemd_service(args.action)
    else:
        raise KoyomiError(f"unsupported platform {sys.platform}")
    return 0


def launchd_service(action: str) -> None:
    domain, target = f"gui/{os.getuid()}", f"gui/{os.getuid()}/{LABEL}"
    alive, info = daemon_state()
    old_pid = info["pid"] if alive and info else None
    if action == "install":
        load_config()
        plist = {
            "Label": LABEL,
            "ProgramArguments": daemon_argv(),
            "RunAtLoad": True,
            "KeepAlive": True,
            "EnvironmentVariables": service_env(),
            "StandardOutPath": str(home() / "launchd.log"),
            "StandardErrorPath": str(home() / "launchd.log"),
        }
        plist_path().parent.mkdir(parents=True, exist_ok=True)
        with open(plist_path(), "wb") as f:
            plistlib.dump(plist, f)
        launchctl("bootout", target)
        for _ in range(50):  # bootout is asynchronous
            if not service_loaded():
                break
            time.sleep(0.2)
        r = launchctl("bootstrap", domain, str(plist_path()))
        if r.returncode != 0:
            raise KoyomiError(f"launchctl bootstrap failed: {r.stderr.strip()}")
        wait_for_daemon(old_pid)
        print(f"installed {plist_path()} and started scheduler")
    elif action == "uninstall":
        launchctl("bootout", target)
        plist_path().unlink(missing_ok=True)
        print("scheduler stopped and launchd agent removed")
    elif action == "start":
        if not plist_path().exists():
            raise KoyomiError("not installed; run: koyomi service install")
        if not service_loaded():
            r = launchctl("bootstrap", domain, str(plist_path()))
            if r.returncode != 0:
                raise KoyomiError(f"launchctl bootstrap failed: {r.stderr.strip()}")
        else:
            launchctl("kickstart", target)
        wait_for_daemon()
        print("scheduler started")
    elif action == "stop":
        launchctl("bootout", target)
        print(
            "scheduler stopped (starts again at next login, or: koyomi service start)"
        )
    elif action == "restart":
        r = launchctl("kickstart", "-k", target)
        if r.returncode != 0:
            raise KoyomiError(
                f"launchctl kickstart failed: {r.stderr.strip() or 'not loaded'}"
            )
        wait_for_daemon(old_pid)
        print("scheduler restarted")
    elif action == "status":
        print_daemon_status()


def systemd_unit() -> str:
    env = "".join(f'Environment="{k}={v}"\n' for k, v in service_env().items())
    return (
        "[Unit]\n"
        "Description=Koyomi job scheduler\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        f"ExecStart={shlex.join(daemon_argv())}\n"
        "Restart=always\n"
        "RestartSec=5\n"
        # Only the daemon: runners in flight survive a restart or reinstall.
        "KillMode=process\n"
        f"{env}\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )


def systemd_service(action: str) -> None:
    if action != "status" and os.geteuid() != 0:
        raise KoyomiError("managing the systemd service needs root")
    alive, info = daemon_state()
    old_pid = info["pid"] if alive and info else None
    if action == "install":
        load_config()
        unit_path().write_text(systemd_unit())
        systemctl("daemon-reload")
        systemctl("enable", UNIT)
        systemctl("restart", UNIT)
        wait_for_daemon(old_pid)
        print(f"installed {unit_path()} and started scheduler")
    elif action == "uninstall":
        systemctl("disable", "--now", UNIT, check=False)
        unit_path().unlink(missing_ok=True)
        systemctl("daemon-reload")
        print("scheduler stopped and systemd unit removed")
    elif action in ("start", "restart"):
        if not unit_path().exists():
            raise KoyomiError("not installed; run: koyomi service install")
        systemctl(action, UNIT)
        wait_for_daemon(old_pid if action == "restart" else None)
        print(f"scheduler {action}ed")
    elif action == "stop":
        systemctl("stop", UNIT)
        print("scheduler stopped (starts again at boot, or: koyomi service start)")
    elif action == "status":
        print_daemon_status()


def print_daemon_status() -> None:
    alive, info = daemon_state()
    if alive and info:
        print(
            f"scheduler: running (pid {info['pid']}, version {info['version']}, "
            f"last tick {fmt_rel(info['last_tick'])})"
        )
        if info.get("hub_error"):
            print(
                f"           cannot reach the hub (last contact "
                f"{fmt_rel(info['last_hub_contact']) or 'never'}): {info['hub_error']}"
            )
    else:
        print("scheduler: NOT running (koyomi service start)")
    manager = "launchd" if sys.platform == "darwin" else "systemd"
    print(
        f"{manager + ':':<10} {'loaded' if service_loaded() else 'not loaded'}; "
        f"log: {service_log_hint()}"
    )
    print(f"home:      {home()}")


# ---------------------------------------------------------------- commands


def schedule_from_args(args, required: bool, tz: ZoneInfo) -> dict | None:
    given = [
        (k, v) for k in ("cron", "every", "at", "in_") if (v := getattr(args, k, None))
    ]
    if len(given) > 1:
        raise KoyomiError("use only one of --cron, --every, --at, --in")
    if not given:
        if required:
            raise KoyomiError("a schedule is required: --cron, --every, --at or --in")
        return None
    kind, val = given[0]
    if kind == "cron":
        Cron(val)
        return {"cron": val}
    if kind == "every":
        if parse_duration(val) < MIN_INTERVAL:
            raise KoyomiError("minimum --every interval is 1m")
        return {"every": val, "anchor": iso(now().replace(microsecond=0))}
    if kind == "at":
        return {"at": iso(parse_when(val, tz))}
    return {
        "at": iso(
            now().replace(microsecond=0) + dt.timedelta(seconds=parse_duration(val))
        )
    }


def parse_env(pairs) -> dict:
    env = {}
    for p in pairs or []:
        if "=" not in p:
            raise KoyomiError(f"invalid --env {p!r}, expected KEY=VALUE")
        k, v = p.split("=", 1)
        env[k] = v
    return env


def resolve_command(args) -> str | None:
    if args.cmd and args.argv_command:
        raise KoyomiError("give the command either with --cmd or after --, not both")
    if args.argv_command:
        return shlex.join(args.argv_command)
    return args.cmd


def resolve_cwd(cfg: dict, host: str, cwd: str | None) -> str:
    """A local job's directory is checked here; another host's must be absolute."""
    if host == cfg["host"]:
        path = str(Path(cwd or os.getcwd()).expanduser().resolve())
        if not Path(path).is_dir():
            raise KoyomiError(f"working directory does not exist: {path}")
        return path
    if not cwd:
        raise KoyomiError(f"--cwd is required for a job on another host ({host})")
    if not cwd.startswith("/"):
        raise KoyomiError(f"--cwd must be an absolute path on {host}")
    return cwd


def fmt_next(job: dict) -> str:
    return f"next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}".rstrip()


def cmd_init(args) -> int:
    if args.hub_local:
        hub_cfg = "local"
    elif args.hub_ssh and args.hub_koyomi:
        hub_cfg = {"command": ssh_hub_command(args.hub_ssh, args.hub_koyomi)}
    else:
        raise KoyomiError("give --hub-local, or both --hub-ssh and --hub-koyomi")
    smtp = {}
    for line in Path(args.smtp_env).read_text().splitlines():
        key, sep, value = line.strip().partition("=")
        if sep and key in SMTP_KEYS:
            smtp[key] = value.strip().strip("'\"")
    missing = [k for k in SMTP_KEYS if not smtp.get(k)]
    if missing:
        raise KoyomiError(f"{args.smtp_env} lacks {', '.join(missing)}")
    cfg = {
        "host": args.host,
        "hub": hub_cfg,
        "email": {
            "smtp_host": smtp["SMTP_HOST"],
            "smtp_port": int(smtp["SMTP_PORT"]),
            "smtp_user": smtp["SMTP_USER"],
            "smtp_password": smtp["SMTP_PASSWORD"],
            "from": args.email_from,
            "to": args.email_to,
        },
    }
    if cfg["hub"] == "local":
        home().mkdir(parents=True, exist_ok=True)
    call_hub(
        cfg,
        "register_host",
        {"name": args.host, "always_on": args.always_on, "platform": sys.platform},
    )
    write_json(config_path(), cfg, private=True)
    where = "this machine" if args.hub_local else args.hub_ssh
    print(
        f"initialized {args.host} ({'always on' if args.always_on else 'runs while awake'}); "
        f"hub: {where}; alerts to {args.email_to}"
    )
    return 0


def cmd_add(args) -> int:
    cfg = load_config()
    host = args.host or cfg["host"]
    command = resolve_command(args)
    if not command:
        raise KoyomiError(
            "a command is required: --cmd 'shell command' or -- program args"
        )
    tz = args.tz or local_tz_name()
    if args.timeout:
        parse_duration(args.timeout)
    job = {
        "id": args.id,
        "host": host,
        "description": args.description or "",
        "command": command,
        "cwd": resolve_cwd(cfg, host, args.cwd),
        "env": parse_env(args.env),
        "schedule": schedule_from_args(args, required=True, tz=zone(tz)),
        "timezone": tz,
        "enabled": not args.disabled,
        "catchup": args.catchup or "once",
        "timeout": args.timeout,
    }
    res = hub("add_job", job=job)
    job = res["job"]
    print(f"added {args.id} on {host}: {describe_schedule(job)}; {fmt_next(job)}")
    if not res["host_live"]:
        print(
            f"warning: host {host} is not reporting; the job runs when its scheduler is up",
            file=sys.stderr,
        )
    return 0


def cmd_update(args) -> int:
    cfg = load_config()
    job = hub("job", job_id=args.id)
    changes: dict = {}
    if args.host:
        changes["host"] = args.host
    host = changes.get("host", job["host"])
    command = resolve_command(args)
    if command:
        changes["command"] = command
    if args.cwd:
        changes["cwd"] = resolve_cwd(cfg, host, args.cwd)
    if args.description is not None:
        changes["description"] = args.description
    if args.tz:
        changes["timezone"] = args.tz
    sched = schedule_from_args(
        args, required=False, tz=zone(changes.get("timezone", job["timezone"]))
    )
    if sched:
        changes["schedule"] = sched
    if args.catchup:
        changes["catchup"] = args.catchup
    if args.timeout:
        changes["timeout"] = (
            None if args.timeout.lower() in ("none", "0") else args.timeout
        )
    if args.env:
        changes["env"] = parse_env(args.env)
    job = hub(
        "update_job", job_id=args.id, changes=changes, unset_env=args.unset_env or []
    )
    print(f"updated {args.id}; {fmt_next(job)}")
    return 0


def cmd_enable(args, enabled: bool) -> int:
    job = hub("set_enabled", job_id=args.id, enabled=enabled)
    print(f"enabled {args.id}; {fmt_next(job)}" if enabled else f"disabled {args.id}")
    return 0


def cmd_delete(args) -> int:
    job = hub("delete_job", job_id=args.id, keep_logs=args.keep_logs)
    if job.get("running"):
        print(
            f"note: run {job['running']['run_id']} on {job['host']} is still in progress",
            file=sys.stderr,
        )
    print(f"deleted {args.id}" + (" (run history kept)" if args.keep_logs else ""))
    return 0


def run_exit_code(rec: dict) -> int:
    if rec["status"] == "success":
        return 0
    return rec["exit_code"] if rec["exit_code"] and rec["exit_code"] > 0 else 1


def follow_run(job_id: str, run_id: str) -> int:
    """Stream a run's output until it ends. Ctrl-C asks the run to stop."""
    offset, stopping, partial = 0, False, ""
    decode = codecs.getincrementaldecoder("utf-8")(errors="replace").decode
    while True:
        try:
            out = hub("log", job_id=job_id, run_id=run_id, offset=offset)
            *lines, partial = (partial + decode(base64.b64decode(out["data"]))).split(
                "\n"
            )
            for line in lines:
                print(render_log_line(line), flush=True)
            offset = out["size"]
            rec = out["run"]
            if rec["status"] not in ACTIVE_STATES:
                break
            time.sleep(1)
        except KeyboardInterrupt:
            if stopping:
                raise
            stopping = True
            print(
                "\nkoyomi: stopping the run (Ctrl-C again to stop following)",
                file=sys.stderr,
            )
            hub("request_stop", job_id=job_id)
    if partial:
        print(render_log_line(partial))
    print(
        f"--- koyomi: {job_id} {rec['status']} on {rec['host']} "
        f"(exit {rec['exit_code']}, {fmt_dur(rec['duration_seconds'])}) run {run_id}",
        file=sys.stderr,
    )
    return run_exit_code(rec)


def cmd_run(args) -> int:
    rec = hub("queue_run", job_id=args.id)
    print(f"queued {args.id} run {rec['run_id']} on {rec['host']}", file=sys.stderr)
    if args.detach:
        return 0
    return follow_run(args.id, rec["run_id"])


def wait_until_idle(job_id: str, seconds: float = 20) -> dict | None:
    """Poll until the runner has closed its run. Returns the final run."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        job = hub("job", job_id=job_id)
        if not job.get("running"):
            return job.get("last_run")
        time.sleep(0.5)
    return None


def cmd_stop(args) -> int:
    run_id = hub("request_stop", job_id=args.id)
    last = wait_until_idle(args.id)
    if last is None:
        print(f"asked {args.id} run {run_id} to stop; still shutting down")
        return 0
    print(
        f"stopped {args.id} run {run_id}: {last['status']}"
        + (f" ({last['error']})" if last.get("error") else "")
    )
    return 0


def job_status_word(job: dict) -> str:
    if job.get("running"):
        return job["running"]["status"]
    if not job.get("enabled"):
        return "disabled"
    if job.get("next_run") is None:
        return "done"
    return "enabled"


def print_table(rows: list[list[str]], headers: list[str]) -> None:
    widths = [max(len(str(x)) for x in col) for col in zip(headers, *rows)]
    for row in [headers, *rows]:
        print("  ".join(str(x).ljust(w) for x, w in zip(row, widths)).rstrip())


def cmd_list(args) -> int:
    jobs = hub("jobs")
    if args.json:
        print(json.dumps(jobs, indent=2))
        return 0
    if not jobs:
        print("no jobs (add one: koyomi add ID --cron '0 9 * * *' --cmd 'echo hi')")
        return 0
    rows = []
    for j in jobs:
        last = j.get("last_run") or {}
        rows.append(
            [
                j["id"],
                j["host"],
                job_status_word(j),
                describe_schedule(j),
                fmt_time(j.get("next_run")),
                f"{last.get('status', '-')} {fmt_rel(last.get('finished_at') or last.get('started_at'))}".strip(),
            ]
        )
    print_table(rows, ["ID", "HOST", "STATE", "SCHEDULE", "NEXT RUN", "LAST RUN"])
    return 0


def cmd_show(args) -> int:
    job = hub("job", job_id=args.id)
    if args.json:
        print(json.dumps(job, indent=2))
        return 0
    last = job.get("last_run")
    print(f"id:          {job['id']}")
    if job.get("description"):
        print(f"description: {job['description']}")
    print(f"host:        {job['host']}")
    print(f"state:       {job_status_word(job)}")
    print(f"command:     {job['command']}")
    print(f"cwd:         {job['cwd']}")
    if job.get("env"):
        print(f"env:         {' '.join(f'{k}={v}' for k, v in job['env'].items())}")
    print(f"schedule:    {describe_schedule(job)}")
    print(f"catchup:     {job['catchup']}   timeout: {job.get('timeout') or 'none'}")
    print(
        f"next run:    {fmt_time(job.get('next_run'))} {fmt_rel(job.get('next_run'))}"
    )
    if job.get("enabled") and job.get("next_run") and "at" not in job["schedule"]:
        t, upcoming = parse_iso(job["next_run"]), []
        for _ in range(3):
            assert t is not None
            t = compute_next(job, t)
            upcoming.append(fmt_time(t))
        print(f"then:        {', '.join(upcoming)}")
    if job.get("running"):
        r = job["running"]
        since = f" since {fmt_time(r['started_at'])}" if r["started_at"] else ""
        print(f"{r['status'] + ':':<13}run {r['run_id']} pid {r['pid']}{since}")
    if last:
        print(
            f"last run:    {last['status']} exit={last['exit_code']} at {fmt_time(last['started_at'])} "
            f"({last['trigger']}) run {last['run_id']}"
        )
        if last.get("error"):
            print(f"last error:  {last['error']}")
    return 0


def cmd_history(args) -> int:
    runs = hub("runs", job_id=args.id, failed=args.failed, limit=args.limit)
    if args.json:
        print(json.dumps(runs, indent=2))
        return 0
    if not runs:
        print("no runs")
        return 0
    rows = [
        [
            r["run_id"],
            r["job_id"],
            r["host"],
            r["trigger"],
            r["status"],
            "-" if r["exit_code"] is None else r["exit_code"],
            fmt_time(r["started_at"]),
            fmt_dur(r["duration_seconds"]),
            r.get("error") or "",
        ]
        for r in runs
    ]
    print_table(
        rows,
        [
            "RUN",
            "JOB",
            "HOST",
            "TRIGGER",
            "STATUS",
            "EXIT",
            "STARTED",
            "DURATION",
            "ERROR",
        ],
    )
    return 0


def cmd_logs(args) -> int:
    if args.daemon or not args.id:
        print("==> hub daemon.log", file=sys.stderr)
        print(hub("daemon_log", lines=args.lines))
        return 0
    out = hub("log", job_id=args.id, run_id=args.run, lines=args.lines)
    rec = out["run"]
    print(
        f"==> run {rec['run_id']} on {rec['host']}: {rec['status']} exit={rec['exit_code']} "
        f"started {fmt_time(rec['started_at'])}",
        file=sys.stderr,
    )
    for line in base64.b64decode(out["data"]).decode(errors="replace").splitlines():
        print(render_log_line(line))
    return 0


def cmd_status(args) -> int:
    cfg = load_config()
    print_daemon_status()
    snap = hub("snapshot")
    print(
        f"hub:       {'this machine' if cfg['hub'] == 'local' else cfg['hub']['command'][-3]}"
    )
    print("hosts:")
    cur = now()
    for h in snap["hosts"]:
        version = (h.get("daemon") or {}).get("version", "-")
        live = "up" if host_live(h, cur) else "NOT REPORTING"
        print(
            f"  {h['name']:<12} {'always on' if h['always_on'] else 'when awake':<11} "
            f"{live:<14} seen {fmt_rel(h['last_seen']) or 'never'}, version {version}"
        )
    jobs = snap["jobs"]
    enabled = [j for j in jobs if j.get("enabled")]
    running = [j for j in jobs if j.get("running")]
    print(
        f"jobs:      {len(jobs)} total, {len(enabled)} enabled, {len(running)} active"
    )
    upcoming = sorted(
        (j for j in enabled if j.get("next_run")), key=lambda j: j["next_run"]
    )
    if upcoming:
        j = upcoming[0]
        print(
            f"next:      {j['id']} on {j['host']} at {fmt_time(j['next_run'])} ({fmt_rel(j['next_run'])})"
        )
    failing = [
        j for j in jobs if (j.get("last_run") or {}).get("status") in ALERT_STATES
    ]
    if failing:
        print("failing:")
        for j in failing:
            lr = j["last_run"]
            print(
                f"  {j['id']} on {j['host']}: {lr['status']} ({lr.get('error')}) at "
                f"{fmt_time(lr['started_at'])} -> koyomi logs {j['id']}"
            )
    unsent = snap["unsent_alerts"]
    if unsent:
        print(
            f"alerts:    {len(unsent)} not emailed yet; last error: {unsent[-1]['last_error']}"
        )
    return 0


# ---------------------------------------------------------------- terminal UI

FAILURE_STATES = ALERT_STATES
# 256-color palette; -1 is the terminal's own foreground. A style name ending
# in "*" is drawn bold. Terminals without 256 colors get plain attributes.
PALETTE = {
    "text": -1,
    "muted": 245,
    "faint": 240,
    "rule": 237,
    "accent": 179,
    "green": 114,
    "red": 203,
    "yellow": 221,
    "cyan": 80,
    "match": (16, 137),  # search hits: (foreground, background)
    "match_now": (16, 221),  # the hit n / N landed on
}
SELECTED_BG = 236
SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
# job state -> (style, glyph); the order is also the "state" sort order
STATE_STYLES = {
    "running": ("cyan", None),  # spinner
    "queued": ("cyan", "◌"),
    "disabled": ("faint", "○"),
    "done": ("faint", "✓"),
    "enabled": ("green", "●"),
}
RUN_STYLES = {
    "success": ("green", "✓"),
    "failed": ("red", "✗"),
    "timeout": ("red", "✗"),
    "interrupted": ("red", "✗"),
    "stopped": ("yellow", "■"),
    "skipped": ("yellow", "↷"),
    "queued": ("cyan", "◌"),
    "running": ("cyan", "●"),
}


def cmd_tui(args) -> int:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise KoyomiError(
            "tui requires an interactive terminal (use list, status, or history instead)"
        )
    try:
        import curses
    except ImportError as e:
        raise KoyomiError(f"terminal UI is unavailable: {e}") from None
    refresh = args.refresh
    if not 0.5 <= refresh <= 60:
        raise KoyomiError("--refresh must be between 0.5 and 60 seconds")
    load_config()
    os.environ.setdefault("ESCDELAY", "25")
    return curses.wrapper(lambda screen: TuiApp(screen, refresh).run())


# (header, key, width); 0 sizes to content, -1 absorbs the leftover width
TUI_COLUMNS = [
    ("JOB", "id", 0),
    ("HOST", "host", 9),
    ("STATE", "state", 10),
    ("SCHEDULE", "schedule", -1),
    ("NEXT", "next", 11),
    ("LAST", "last", 13),
    ("WHEN", "when", 11),
]
TUI_GAP = 2
TUI_BODY_TOP = 6
TUI_KEYS = [
    (
        "jobs",
        [
            ("j / k, ↑ ↓", "move selection"),
            ("g / G", "first / last job"),
            ("PgUp / PgDn", "page up / down"),
            ("Enter", "open the job: its runs and their logs"),
            ("e", "enable / disable (an active run keeps going)"),
            ("r", "run now, on the job's host (asks first)"),
            ("x", "stop the active run (asks first)"),
            ("d", "delete the job and its history (asks first)"),
            ("/", "search jobs; n / N next / previous match; Esc clears"),
            ("s", "sort by id / host / next run / state"),
            ("D", "scheduler log of every host (live)"),
            ("q", "quit"),
        ],
    ),
    (
        "job",
        [
            ("j / k, ↑ ↓", "pick a run; its log shows on the right"),
            ("g / G", "newest / oldest run"),
            ("Enter", "read the whole log of the run (live while running)"),
            ("e, r, x", "enable / disable, run now, stop, as on the job list"),
            ("q, Esc", "back to the job list"),
        ],
    ),
    (
        "log",
        [
            ("j / k", "scroll"),
            ("Ctrl-D / U", "page down / up"),
            ("g / G", "top / end"),
            ("f", "follow new output"),
            ("/", "search; n / N next / previous match"),
            ("q, Esc", "close (Esc clears a search first)"),
        ],
    ),
]
DASHBOARD_HINTS = [
    ("j/k", "move"),
    ("⏎", "open"),
    ("e", "toggle"),
    ("r", "run"),
    ("x", "stop"),
    ("/", "search"),
    ("s", "sort"),
    ("?", "help"),
    ("q", "quit"),
]
JOB_HINTS = [
    ("j/k", "runs"),
    ("⏎", "full log"),
    ("r", "run"),
    ("x", "stop"),
    ("e", "toggle"),
    ("esc", "back"),
    ("?", "help"),
]
PAGER_HINTS = [
    ("j/k", "scroll"),
    ("^D/^U", "page"),
    ("g/G", "top/end"),
    ("f", "follow"),
    ("/", "search"),
    ("q", "close"),
]
RUNS_WIDTH = 38  # the run list beside the log on the job page
PREVIEW_LINES = 200  # log tail fetched for the job page; Enter downloads it all
QUIET_KEYS = 0.15  # seconds without a key before the job page fetches a preview
SORTS = ("id", "host", "next", "state")


DAEMON_LOG_LINES = 500


def log_segments(line: str) -> list[tuple[str, str]]:
    """A run log line for the UI: its stamp as a faint local-time gutter."""
    stamp, text = split_log_line(line.replace("\t", "    "))
    if stamp:
        return [(stamp + "  ", "faint"), (text, "text")]
    return [(text, "text")]


def line_text(segments) -> str:
    return "".join(text for text, _ in segments)


def search_ranges(text: str, query: str) -> list[tuple[int, int]]:
    """Where query occurs in text; smartcase, as in vim: an uppercase letter in
    the query makes it case-sensitive."""
    if not query:
        return []
    if query == query.lower():
        text = text.lower()
    ranges, start = [], text.find(query)
    while start >= 0:
        ranges.append((start, start + len(query)))
        start = text.find(query, start + len(query))
    return ranges


def mark(segments, hits) -> list[tuple[str, str]]:
    """Restyle the (start, end, current) hits of segments as search matches."""
    if not hits:
        return segments
    out, pos = [], 0
    for text, style in segments:
        cut = 0
        for start, end, current in hits:
            a, b = max(start - pos, cut), min(end - pos, len(text))
            if a < b:
                out += [
                    (text[cut:a], style),
                    (text[a:b], "match_now" if current else "match"),
                ]
                cut = b
        out.append((text[cut:], style))
        pos += len(text)
    return [(text, style) for text, style in out if text]


def step_to(items, origin, forward: bool, key) -> int:
    """Index of the first item after origin (before it, backwards), wrapping."""
    order = range(len(items)) if forward else range(len(items) - 1, -1, -1)
    for i in order:
        if (key(items[i]) > origin) if forward else (key(items[i]) < origin):
            return i
    return order[0]


class HubWorker:
    """Runs hub calls on a background thread, one at a time, so the UI never
    waits on ssh. A task already queued under the same key is not queued again."""

    def __init__(self):
        self.tasks: queue.Queue = queue.Queue()
        self.results: queue.Queue = queue.Queue()
        self.pending: set = set()
        threading.Thread(target=self.serve, daemon=True).start()

    def submit(self, key, fn) -> None:
        if key not in self.pending:
            self.pending.add(key)
            self.tasks.put((key, fn))

    def serve(self) -> None:
        while True:
            key, fn = self.tasks.get()
            try:
                self.results.put((key, fn(), None))
            except Exception as e:  # handed to the UI thread, which re-raises bugs
                self.results.put((key, None, e))

    def finished(self):
        """(key, result, error) of every task done since the last call."""
        while True:
            try:
                key, result, error = self.results.get_nowait()
            except queue.Empty:
                return
            self.pending.discard(key)
            if error is not None and not isinstance(error, KoyomiError):
                raise error
            yield key, result, error


class TuiApp:
    """Small curses dashboard over the hub, the same data the CLI shows."""

    def __init__(self, screen, refresh: float):
        self.screen, self.refresh = screen, refresh
        self.hosts: list[dict] = []
        self.unsent = 0
        self.all_jobs: list[dict] = []
        self.jobs: list[dict] = []
        self.selected_id: str | None = None
        self.scroll = 0
        self.page: dict | None = None  # the open job: id, runs, picked run
        self.logs: dict[tuple, dict] = {}  # (job, run) -> its log tail
        self.loads = 0  # completed reloads; an active run's log is per reload
        self.sort = "id"
        self.query = ""  # the active search
        self.typed = ""  # the search being typed after "/"
        self.origin: object = None  # where the search started; Esc goes back
        self.mode: str | None = None  # None | "search" | "confirm"
        self.last_key = 0.0
        self.worker = HubWorker()
        self.confirm: tuple[str, object, str] | None = None
        self.message: tuple[str, str, float] | None = None
        self.pager: dict | None = None
        self.palette = False
        self.pairs: dict[tuple[str, bool], int] = {}

    # -------------------------------------------------------------- plumbing

    def run(self) -> int:
        import curses

        with contextlib.suppress(curses.error):
            curses.curs_set(0)
        with contextlib.suppress(curses.error, AttributeError):
            curses.set_escdelay(25)
        self.screen.keypad(True)
        self.screen.timeout(80)  # input poll; data reloads on its own clock
        self.init_colors()
        self.reload()
        next_load = time.monotonic() + self.refresh
        dirty, frame = True, ""
        while True:
            for key, result, error in self.worker.finished():
                self.apply(key, result, error)
                dirty = True
            if self.animating() and self.spinner() != frame:
                frame, dirty = self.spinner(), True
            if dirty:
                self.draw()
                dirty = False
            key = self.screen.getch()
            if key != -1:
                self.last_key = time.monotonic()
                if self.handle_key(key):
                    return 0
                dirty = True
            elif time.monotonic() - self.last_key >= QUIET_KEYS:
                self.fetch_preview()  # not while flicking through runs
            if time.monotonic() >= next_load:
                self.reload()
                next_load = time.monotonic() + self.refresh

    def init_colors(self) -> None:
        import curses

        with contextlib.suppress(curses.error):
            curses.start_color()
            curses.use_default_colors()
            self.palette = curses.has_colors() and curses.COLORS >= 256

    def style(self, name: str, selected: bool = False) -> int:
        import curses

        bold = name.endswith("*")
        name = name.rstrip("*")
        if selected and name == "faint":  # too close to the selection background
            name = "muted"
        attr = curses.A_BOLD if bold else 0
        color = PALETTE[name]
        if not self.palette:
            if name in ("muted", "faint", "rule"):
                attr |= curses.A_DIM
            if name == "match_now":
                attr |= curses.A_BOLD
            return attr | (
                curses.A_REVERSE if selected or name.startswith("match") else 0
            )
        if (name, selected) not in self.pairs:
            fg, bg = color if isinstance(color, tuple) else (color, -1)
            if selected and not isinstance(color, tuple):
                bg = SELECTED_BG
            n = len(self.pairs) + 1
            curses.init_pair(n, fg, bg)
            self.pairs[name, selected] = curses.color_pair(n)
        return attr | self.pairs[name, selected]

    @staticmethod
    def spinner() -> str:
        return SPINNER[int(time.monotonic() * 10) % len(SPINNER)]

    def animating(self) -> bool:
        if self.pager:
            return self.pager["loading"]
        return not self.loads or any(
            (j.get("running") or {}).get("status") == "running" for j in self.jobs
        )

    def reload(self) -> None:
        """Ask the worker for fresh state; apply() takes it when it arrives."""
        job_id = self.page["job_id"] if self.page else None

        def fetch():
            snap = hub("snapshot")
            runs = None
            if job_id in {j["id"] for j in snap["jobs"]}:
                runs = self.load_runs(job_id)
            return snap, job_id, runs

        self.worker.submit(("reload",), fetch)
        pager = self.pager
        if pager and pager["kind"] == "run" and pager["active"]:
            self.fetch_run_log()
        elif pager and pager["kind"] == "daemon":
            self.worker.submit(
                ("daemon_log",), lambda: hub("daemon_log", lines=DAEMON_LOG_LINES)
            )

    def apply(self, key: tuple, result, error: KoyomiError | None) -> None:
        """Take a finished worker task into the UI state."""
        kind = key[0]
        if kind == "reload":
            if error:  # hub unreachable: keep showing the last data
                self.notify(f"refresh failed: {error}", "red")
                return
            snap, job_id, runs = result
            self.loads += 1
            self.hosts, self.all_jobs = snap["hosts"], snap["jobs"]
            self.unsent = len(snap["unsent_alerts"])
            if self.page and not self.page_job:
                self.notify(f"{self.page['job_id']} no longer exists", "yellow")
                self.page = None
            elif self.page and runs is not None and self.page["job_id"] == job_id:
                self.page["runs"] = runs
            self.apply_view()
        elif kind == "preview":
            status, loads, out = result or (None, None, None)
            if error:
                lines = [[(str(error), "red")]]
            else:
                text = base64.b64decode(out["data"]).decode(errors="replace")
                lines = [log_segments(line) for line in text.splitlines()]
            self.logs[key[1:]] = {"status": status, "loads": loads, "lines": lines}
        elif kind == "log":
            pager = self.pager
            if not pager or pager.get("run") != key[1:]:
                return  # closed, or another log opened since
            pager["loading"] = False
            if error:
                self.notify(f"log download failed: {error}", "red")
                return
            pager["size"] = result["size"]
            pager["active"] = result["run"]["status"] in ACTIVE_STATES
            self.append_log(pager, pager["decode"](base64.b64decode(result["data"])))
        elif kind == "daemon_log":
            if self.pager and self.pager["kind"] == "daemon":
                self.pager["loading"] = False
                if error:
                    self.notify(str(error), "red")
                    return
                self.pager["lines"] = [[(line, "text")] for line in result.splitlines()]
                self.pager["version"] += 1

    @staticmethod
    def load_runs(job_id: str) -> list[dict]:
        return list(reversed(hub("runs", job_id=job_id, failed=False, limit=200)))

    def apply_view(self) -> None:
        """Re-derive the visible job list, keeping the selection where it can."""
        keys = {
            "id": lambda j: j["id"],
            "host": lambda j: (j["host"], j["id"]),
            "next": lambda j: (j.get("next_run") is None, j.get("next_run") or ""),
            "state": lambda j: (
                list(STATE_STYLES).index(job_status_word(j)),
                j["id"],
            ),
        }
        self.jobs = sorted(self.all_jobs, key=keys[self.sort])
        if not self.jobs:
            self.selected_id = None
        elif self.selected_id not in {j["id"] for j in self.jobs}:
            self.selected_id = self.jobs[min(self.index, len(self.jobs) - 1)]["id"]

    @staticmethod
    def haystack(job: dict) -> str:
        """What a search of the job list looks through."""
        return " ".join(
            (job["id"], job["host"], job.get("description") or "", job["command"])
        )

    @property
    def index(self) -> int:
        for i, job in enumerate(self.jobs):
            if job["id"] == self.selected_id:
                return i
        return 0

    @property
    def job(self) -> dict | None:
        return self.jobs[self.index] if self.jobs else None

    def notify(self, text: str, color: str = "text") -> None:
        self.message = (text, color, time.monotonic())

    # ----------------------------------------------------------------- draw

    def add(self, y: int, x: int, text: str, attr: int = 0) -> None:
        height, width = self.screen.getmaxyx()
        if 0 <= y < height and 0 <= x < width - 1:
            with contextlib.suppress(Exception):
                self.screen.addnstr(y, x, text, width - x - 1, attr)

    def fill(self, y: int, attr: int) -> None:
        _, width = self.screen.getmaxyx()
        self.add(y, 0, " " * (width - 1), attr)

    def put(
        self, y: int, x: int, segments, limit: int | None = None, selected=False
    ) -> int:
        """Draw (text, style) segments from x, clipping to limit cells with an
        ellipsis; returns the x after the last cell drawn."""
        for text, name in segments:
            if limit is not None:
                if limit <= 0:
                    break
                if len(text) > limit:
                    text = text[: limit - 1] + "…"
                limit -= len(text)
            self.add(y, x, text, self.style(name, selected))
            x += len(text)
        return x

    @staticmethod
    def span(segments) -> int:
        return sum(len(text) for text, _ in segments)

    def put_right(self, y: int, right: int, segments) -> None:
        self.put(y, right - self.span(segments), segments)

    def rule(self, y: int, title: str = "") -> None:
        _, width = self.screen.getmaxyx()
        if title:
            end = self.put(y, 1, [("── ", "rule"), (title, "text*"), (" ", "rule")])
            self.add(y, end, "─" * max(0, width - end - 2), self.style("rule"))
        else:
            self.add(y, 1, "─" * (width - 3), self.style("rule"))

    def hints(self, y: int, pairs, right: int) -> None:
        """Key hints from the left, as many as fit before column right."""
        x = 1
        for key, label in pairs:
            seg = [(key, "accent"), (" " + label, "muted")]
            if x + self.span(seg) > right:
                break
            x = self.put(y, x, seg) + 3

    def brand(self, trail: str = "") -> int:
        x = self.put(0, 1, [("暦", "accent*")]) + 2  # a wide glyph: two cells
        segs = [("koyomi", "text*")]
        if trail:
            segs += [("  ›  ", "faint"), (trail, "text")]
        return self.put(0, x, segs)

    def draw(self) -> None:
        self.screen.erase()
        if self.pager:
            self.draw_pager()
        elif self.page:
            self.draw_job_page()
        else:
            self.draw_dashboard()
        self.screen.refresh()

    def body_rows(self, height: int) -> int:
        return max(1, height - TUI_BODY_TOP - 3)

    def draw_dashboard(self) -> None:
        height, width = self.screen.getmaxyx()
        self.draw_header(width)
        columns = self.layout(width)
        body = self.body_rows(height)
        self.clamp_scroll(body)
        x = 2
        for header, _, w in columns:
            self.add(TUI_BODY_TOP - 2, x, header[:w], self.style("faint"))
            x += w + TUI_GAP
        self.rule(TUI_BODY_TOP - 1)
        if not self.loads:
            self.put(
                TUI_BODY_TOP + 1,
                2,
                [(f"{self.spinner()} ", "cyan"), ("reading the hub…", "muted")],
            )
        elif not self.jobs:
            self.put(TUI_BODY_TOP + 1, 2, [("no jobs yet", "muted")])
            self.put(
                TUI_BODY_TOP + 2,
                2,
                [
                    ("add one with  ", "faint"),
                    ("koyomi add ID --cron '…' -- CMD", "accent"),
                ],
            )
        for row, job in enumerate(self.jobs[self.scroll : self.scroll + body]):
            self.draw_row(TUI_BODY_TOP + row, columns, job)
        hidden = len(self.jobs) - self.scroll - body
        if hidden > 0:
            self.put(TUI_BODY_TOP + body, 2, [(f"↓ {hidden} more", "faint")])
        self.draw_footer(height, DASHBOARD_HINTS)

    def draw_header(self, width: int) -> None:
        self.brand()
        cur = now()
        hosts = []
        for h in self.hosts:
            if host_live(h, cur):
                hosts += [("● ", "green"), (h["name"], "muted")]
            elif h["always_on"]:
                hosts += [("● ", "red"), (h["name"], "red"), (" down", "red*")]
            else:
                hosts += [("○ ", "faint"), (h["name"], "faint"), (" away", "faint")]
            hosts.append(("   ", "text"))
        self.put_right(0, width - 2, hosts[:-1])

        active = sum(bool(j.get("running")) for j in self.all_jobs)
        failing = sum(
            (j.get("last_run") or {}).get("status") in FAILURE_STATES
            for j in self.all_jobs
        )
        total = len(self.all_jobs)
        enabled = sum(bool(j.get("enabled")) for j in self.all_jobs)
        parts = [
            [(str(total), "text*"), (" job" + ("s" if total != 1 else ""), "muted")],
            [(str(enabled), "text*"), (" enabled", "muted")],
            [(str(active), "cyan*"), (" active", "cyan")]
            if active
            else [("0 active", "faint")],
            [(str(failing), "red*"), (" failing", "red")]
            if failing
            else [("0 failing", "faint")],
        ]
        if self.unsent:
            parts.append(
                [
                    ("▲ ", "red"),
                    (str(self.unsent), "red*"),
                    (
                        " alert" + ("s" if self.unsent != 1 else "") + " not emailed",
                        "red",
                    ),
                ]
            )
        x = 2
        for i, seg in enumerate(parts):
            if i:
                x = self.put(2, x, [("  ·  ", "rule")])
            x = self.put(2, x, seg)

        self.put_right(2, width - 2, [("sort ", "faint"), (self.sort, "muted")])

    def layout(self, width: int) -> list[tuple[str, str, int]]:
        """Drop optional columns right-to-left, then fit JOB to the ids on screen."""
        columns = list(TUI_COLUMNS)
        natural = min(28, max(6, *(len(j["id"]) for j in self.jobs), len("JOB")))
        while len(columns) > 2:
            used = sum(max(w, natural) for _, _, w in columns)
            used += TUI_GAP * len(columns) + 2
            if width - used >= 16:
                break
            columns.pop()
        fixed = sum(w for _, _, w in columns if w > 0) + TUI_GAP * len(columns) + 2
        for i, (header, key, w) in enumerate(columns):
            if w == 0:
                columns[i] = (header, key, natural)
                fixed += natural
        sched = max((len(describe_schedule(j)) for j in self.jobs), default=10)
        for i, (header, key, w) in enumerate(columns):
            if w == -1:
                columns[i] = (header, key, max(10, min(sched, width - fixed - 1)))
        return columns

    def draw_row(self, y: int, columns, job: dict) -> None:
        selected = job["id"] == self.selected_id
        if selected:
            self.fill(y, self.style("text", True))
            self.add(y, 0, "▌", self.style("accent", True))
        cells = self.row_cells(job)
        query = self.typed if self.mode == "search" else self.query
        x = 2
        for _, key, w in columns:
            segs = cells[key]
            if selected and key == "id":
                segs = [(text, "text*") for text, _ in segs]
            if key in ("id", "host"):
                hits = search_ranges(line_text(segs), query)
                segs = mark(segs, [(a, b, selected) for a, b in hits])
            self.put(y, x, segs, w, selected)
            x += w + TUI_GAP

    def row_cells(self, job: dict) -> dict[str, list[tuple[str, str]]]:
        state = job_status_word(job)
        color, glyph = STATE_STYLES[state]
        last = job.get("last_run") or {}
        status = last.get("status")
        if job.get("next_run"):
            nxt = [(fmt_rel(job["next_run"]), "text")]
        elif not job.get("enabled"):
            nxt = [("paused", "faint")]
        else:
            nxt = [("—", "faint")]
        if status:
            run_color, run_glyph = RUN_STYLES.get(status, ("muted", "·"))
            last_cell = [(f"{run_glyph} ", run_color), (status, run_color)]
        else:
            last_cell = [("—", "faint")]
        schedule = describe_schedule(job)
        main, _, zone_name = schedule.partition(" (")
        sched = [(main, "text" if job.get("enabled") else "muted")]
        if zone_name:
            sched.append((" " + zone_name.rstrip(")"), "faint"))
        return {
            "id": [(job["id"], "text" if job.get("enabled") else "muted")],
            "host": [(job["host"], "muted")],
            "state": [(f"{glyph or self.spinner()} ", color), (state, color)],
            "schedule": sched,
            "next": nxt,
            "last": last_cell,
            "when": [
                (
                    fmt_rel(last.get("finished_at") or last.get("started_at")) or "—",
                    "muted" if last else "faint",
                )
            ],
        }

    def draw_footer(self, height: int, hints) -> None:
        _, width = self.screen.getmaxyx()
        if self.mode == "confirm" and self.confirm:
            question, _, tone = self.confirm
            self.put(
                height - 2,
                1,
                [
                    ("? ", f"{tone}*"),
                    (question, f"{tone}*"),
                    ("   y", "accent"),
                    (" yes  ", "muted"),
                    ("n", "accent"),
                    (" no", "muted"),
                ],
            )
        elif self.message:
            text, color, at = self.message
            if time.monotonic() - at > 6:
                self.message = None
            else:
                self.put(height - 2, 1, [("› ", "faint"), (text, color)], width - 3)
        if self.mode == "search":  # the prompt takes the hints' place, as in vim
            self.put(
                height - 1, 1, [("/", "accent*"), (self.typed, "text"), ("▏", "accent")]
            )
            if self.typed:
                count = self.match_count()
                self.put_right(height - 1, width - 2, count or [("no match", "red")])
            return
        right = []
        if self.query and ("/", "search") in hints:
            right = [("/", "accent"), (self.query, "yellow"), ("  ", "text")]
            right += self.match_count() or [("no match", "red")]
            hints = [*hints[:-2], ("n/N", "next/prev"), *hints[-2:]]
        self.hints(height - 1, hints, width - 2 - self.span(right) - 3)
        self.put_right(height - 1, width - 2, right)

    def match_count(self) -> list[tuple[str, str]]:
        """ "3 of 17" for the search on screen; [] when nothing matches."""
        if self.pager:
            total, n = len(self.pager_matches()), self.pager["match"]
        else:
            matches = self.job_matches()
            total = len(matches)
            n = matches.index(self.index) if self.index in matches else None
        if not total:
            return []
        return [
            (str(n + 1) if n is not None else "–", "muted"),
            (f" of {total}", "faint"),
        ]

    # ------------------------------------------------------------- job page

    @property
    def page_job(self) -> dict | None:
        assert self.page is not None
        return next((j for j in self.all_jobs if j["id"] == self.page["job_id"]), None)

    @property
    def page_run(self) -> dict | None:
        """The picked run; None as the pick means the newest, whichever it is."""
        assert self.page is not None
        runs, picked = self.page["runs"], self.page["run_id"]
        return next((r for r in runs if r["run_id"] == picked), None) or (
            runs[0] if runs else None
        )

    def open_job(self, job_id: str) -> None:
        try:
            runs = self.load_runs(job_id)
        except KoyomiError as e:
            self.notify(str(e), "red")
            return
        self.page = {"job_id": job_id, "runs": runs, "run_id": None, "scroll": 0}

    def move_run(self, delta: int) -> None:
        assert self.page is not None
        runs = self.page["runs"]
        if runs:
            i = runs.index(self.page_run) + delta
            i = max(0, min(i, len(runs) - 1))
            self.page["run_id"] = runs[i]["run_id"] if i else None

    def fetch_preview(self) -> None:
        """Fetch the tail of the picked run's log unless the cached one is current."""
        run = self.page_run if self.page and not self.pager else None
        if not run:
            return
        job_id, run_id, status = run["job_id"], run["run_id"], run["status"]
        cached = self.logs.get((job_id, run_id))
        if (
            cached
            and cached["status"] == status
            and (status not in ACTIVE_STATES or cached["loads"] == self.loads)
        ):
            return
        loads = self.loads
        self.worker.submit(
            ("preview", job_id, run_id),
            lambda: (
                status,
                loads,
                hub("log", job_id=job_id, run_id=run_id, lines=PREVIEW_LINES),
            ),
        )

    @staticmethod
    def run_when(run: dict) -> str:
        t = parse_iso(run["started_at"] or run["created_at"])
        return t.astimezone().strftime("%b %d %H:%M")

    @staticmethod
    def run_duration(run: dict) -> str:
        if run["duration_seconds"] is not None:
            return fmt_dur(run["duration_seconds"])
        if run["status"] == "running" and run["started_at"]:
            return fmt_dur((now() - parse_iso(run["started_at"])).total_seconds())
        return "—"

    def run_glyph(self, run: dict) -> tuple[str, str]:
        color, glyph = RUN_STYLES.get(run["status"], ("muted", "·"))
        return (self.spinner() if run["status"] == "running" else glyph), color

    def job_facts(self, job: dict) -> list[tuple[str, list[tuple[str, str]]]]:
        main, _, zone_name = describe_schedule(job).partition(" (")
        schedule = [(main, "text")]
        if zone_name:
            schedule.append((" " + zone_name.rstrip(")"), "faint"))
        if job.get("next_run"):
            schedule += [
                ("   next ", "faint"),
                (fmt_rel(job["next_run"]), "text"),
                (f"  {fmt_time(job['next_run'])}", "faint"),
            ]
        elif not job.get("enabled"):
            schedule.append(("   paused", "yellow"))
        where = [
            (f"{job['host']}:{job['cwd']}", "muted"),
            ("   timeout ", "faint"),
            (str(job.get("timeout") or "none"), "muted"),
            ("   catchup ", "faint"),
            (job["catchup"], "muted"),
        ]
        if job.get("env"):
            where += [("   env ", "faint"), (" ".join(job["env"]), "muted")]
        facts = [
            ("command", [("$ ", "accent"), (job["command"], "text")]),
            ("schedule", schedule),
            ("where", where),
        ]
        if job.get("description"):
            facts.insert(0, ("about", [(job["description"], "text")]))
        return facts

    def draw_job_page(self) -> None:
        assert self.page is not None
        height, width = self.screen.getmaxyx()
        job = self.page_job
        if job is None:  # deleted; reload() closes the page
            return
        self.brand(job["id"])
        state = job_status_word(job)
        color, glyph = STATE_STYLES[state]
        self.put_right(
            0, width - 2, [(f"{glyph or self.spinner()} ", color), (state, color)]
        )
        y = 2
        for label, segments in self.job_facts(job):
            self.put(y, 2, [(f"{label:>8}", "faint")])
            self.put(y, 12, segments, width - 14)
            y += 1
        top, bottom = y + 1, height - 3  # panes fill the rows between
        if width >= 100:
            self.draw_runs(top, 2, RUNS_WIDTH, bottom - top + 1)
            sep = 2 + RUNS_WIDTH + 2
            for row in range(top, bottom + 1):
                self.add(row, sep, "│", self.style("rule"))
            self.draw_run_log(top, sep + 3, width - sep - 5, bottom - top + 1)
        else:
            rows = min(len(self.page["runs"]), 5) + 2
            self.draw_runs(top, 2, width - 4, rows)
            self.draw_run_log(top + rows + 1, 2, width - 4, bottom - top - rows)
        self.draw_footer(height, JOB_HINTS)

    def draw_runs(self, top: int, x: int, width: int, height: int) -> None:
        assert self.page is not None
        runs = self.page["runs"]
        self.put(top, x, [("RUNS", "faint"), (f"  {len(runs)}", "faint")])
        self.add(top + 1, x, "─" * width, self.style("rule"))
        if not runs:
            self.put(top + 2, x, [("no runs yet", "muted")])
            self.put(top + 3, x, [("r", "accent"), (" runs it now", "faint")])
            return
        body = max(1, height - 2)
        picked = runs.index(self.page_run)
        scroll = max(0, min(self.page["scroll"], len(runs) - body))
        scroll = min(scroll, picked)
        scroll = max(scroll, picked - body + 1)
        self.page["scroll"] = scroll
        for row, run in enumerate(runs[scroll : scroll + body]):
            y, selected = top + 2 + row, run is runs[picked]
            if selected:
                self.add(y, x - 2, " " * (width + 2), self.style("text", True))
                self.add(y, x - 2, "▌", self.style("accent", True))
            glyph, color = self.run_glyph(run)
            segments = [
                (f"{glyph} ", color),
                (self.run_when(run), "text*" if selected else "text"),
                (f"  {self.run_duration(run):>7}", "muted"),
                (f"  {run['trigger']}", "faint"),
            ]
            self.put(y, x, segments, width, selected)

    def draw_run_log(self, top: int, x: int, width: int, height: int) -> None:
        run = self.page_run
        if run is None or height < 3:
            return
        glyph, color = self.run_glyph(run)
        head = [(f"{glyph} ", color), (run["status"], f"{color}*")]
        if run["exit_code"] is not None:
            head += [("   exit ", "faint"), (str(run["exit_code"]), "text")]
        head += [
            ("   ", "text"),
            (self.run_duration(run), "text"),
            ("   ", "text"),
            (run["trigger"], "muted"),
            ("   ", "text"),
            (fmt_time(run["started_at"] or run["created_at"]), "muted"),
            (f"   {run['run_id']}", "faint"),
        ]
        self.put(top, x, head, width)
        y = top + 1
        if run.get("error"):
            self.put(y, x, [(run["error"], "red")], width)
            y += 1
        self.add(y, x, "─" * width, self.style("rule"))
        y += 1
        rows = top + height - y
        cached = self.logs.get((run["job_id"], run["run_id"]))
        lines = cached["lines"] if cached else None
        if lines is None:
            self.put(y, x, [("loading…", "faint")])
            return
        if not lines:
            empty = {
                "queued": "waiting for the host to start it",
                "running": "no output yet",
            }.get(run["status"], "no output")
            self.put(y, x, [(empty, "muted")])
            return
        if len(lines) > rows:  # show the tail, as the end is what matters
            earlier = len(lines) - rows + 1
            more = "+" if len(lines) >= PREVIEW_LINES else ""
            self.put(
                y,
                x,
                [
                    (f"↑ {earlier}{more} earlier lines   ", "faint"),
                    ("⏎", "accent"),
                    (" full log", "faint"),
                ],
            )
            y, lines = y + 1, lines[-(rows - 1) :]
        for i, line in enumerate(lines):
            self.put(y + i, x, line, width)

    # ---------------------------------------------------------------- pager

    def open_pager(self, title: str, kind: str, lines=None, **extra) -> None:
        """A full-screen view of lines, each a list of (text, style) segments.
        Without lines it shows "downloading" until the worker delivers them."""
        self.pager = {
            "title": title,
            "kind": kind,  # "static" | "daemon" | "run"
            "lines": lines or [],
            "partial": "",  # a run log's last line, still being written
            "loading": lines is None,
            "version": 0,  # bumped when the lines change; keys the match cache
            "scroll": 0,
            "follow": False,
            "match": None,  # index into pager_matches() that n / N landed on
            **extra,
        }

    def open_run_log(self, run: dict) -> None:
        active = run["status"] in ACTIVE_STATES
        self.open_pager(
            f"{run['job_id']}  ›  {self.run_when(run)}",
            "run",
            run=(run["job_id"], run["run_id"]),
            size=0,
            active=active,
            follow=active,
            decode=codecs.getincrementaldecoder("utf-8")(errors="replace").decode,
        )
        self.fetch_run_log()

    def fetch_run_log(self) -> None:
        """Download the open run log from where the last download ended."""
        assert self.pager is not None
        (job_id, run_id), offset = self.pager["run"], self.pager["size"]
        self.worker.submit(
            ("log", job_id, run_id),
            lambda: hub("log", job_id=job_id, run_id=run_id, offset=offset),
        )

    @staticmethod
    def append_log(pager: dict, text: str) -> None:
        *done, pager["partial"] = (pager["partial"] + text).split("\n")
        pager["lines"].extend(log_segments(line) for line in done)
        pager["version"] += 1

    def pager_lines(self) -> list:
        assert self.pager is not None
        if self.pager["partial"]:
            return self.pager["lines"] + [log_segments(self.pager["partial"])]
        return self.pager["lines"]

    def page_size(self) -> int:
        return max(1, self.screen.getmaxyx()[0] - 4)

    def draw_pager(self) -> None:
        assert self.pager is not None
        pager = self.pager
        height, width = self.screen.getmaxyx()
        lines, body = self.pager_lines(), self.page_size()
        max_scroll = max(0, len(lines) - body)
        if pager["follow"]:
            pager["scroll"] = max_scroll
        pager["scroll"] = max(0, min(pager["scroll"], max_scroll))
        top = pager["scroll"]
        self.brand(pager["title"])
        if pager["loading"]:
            pos = [(f"{self.spinner()} ", "cyan"), ("downloading…", "cyan")]
        elif not lines:
            pos = []
        else:
            pos = [
                (f"{top + 1}–{min(len(lines), top + body)}", "muted"),
                (f" of {len(lines)}", "faint"),
            ]
            if pager["follow"] and top == max_scroll:
                pos = [("● ", "cyan"), ("live", "cyan"), ("   ", "text")] + pos
        self.put_right(0, width - 2, pos)
        self.rule(1)
        hits: dict[int, list] = {}
        for n, (li, start, end) in enumerate(self.pager_matches()):
            if top <= li < top + body:
                hits.setdefault(li, []).append((start, end, n == pager["match"]))
        for i, line in enumerate(lines[top : top + body]):
            self.put(i + 2, 2, mark(line, hits.get(top + i, [])), width - 4)
        if not lines and not pager["loading"]:
            self.put(3, 2, [("nothing here yet", "muted")])
        self.draw_footer(height, PAGER_HINTS)

    def pager_matches(self) -> list[tuple[int, int, int]]:
        """Every (line, start, end) the search hits; cached per query and content."""
        assert self.pager is not None
        query = self.typed if self.mode == "search" else self.query
        key = (query, self.pager["version"], self.pager["partial"])
        if self.pager.get("matches_key") != key:
            self.pager["matches_key"] = key
            self.pager["matches"] = [
                (li, start, end)
                for li, line in enumerate(self.pager_lines())
                for start, end in search_ranges(line_text(line), query)
            ]
        return self.pager["matches"]

    def current_match(self) -> tuple[int, int, int] | None:
        assert self.pager is not None
        matches, n = self.pager_matches(), self.pager["match"]
        return matches[n] if n is not None and n < len(matches) else None

    def show_match(self, n: int | None) -> None:
        """Land on match n, scrolling it into view a third of the way down."""
        assert self.pager is not None
        self.pager["match"] = n
        if n is None:
            return
        line = self.pager_matches()[n][0]
        top, body = self.pager["scroll"], self.page_size()
        if not top <= line < top + body:
            self.pager["scroll"] = max(0, line - body // 3)
        self.pager["follow"] = False

    def pager_step(self, forward: bool, origin: tuple[int, int] | None = None) -> None:
        """n / N: the next match after the current one (or after origin), wrapping."""
        assert self.pager is not None
        matches = self.pager_matches()
        if not matches:
            self.pager["match"] = None
            return
        if origin is None:
            here = self.current_match()
            origin = here[:2] if here else (self.pager["scroll"], -1)
        n = step_to(matches, origin, forward, key=lambda m: m[:2])
        self.show_match(n)

    def handle_pager_key(self, key: int) -> None:
        import curses

        assert self.pager is not None
        pager, body = self.pager, self.page_size()
        if key == 27 and self.query:
            self.query, pager["match"] = "", None
            return
        if key in (ord("q"), 27, curses.KEY_LEFT):
            self.pager = None
            return
        if pager["loading"]:
            return  # nothing to move through yet
        moves = {
            curses.KEY_DOWN: 1,
            ord("j"): 1,
            curses.KEY_UP: -1,
            ord("k"): -1,
            curses.KEY_NPAGE: body,
            4: body,  # Ctrl-D
            curses.KEY_PPAGE: -body,
            21: -body,  # Ctrl-U
        }
        if key in moves:
            pager["scroll"] += moves[key]
            pager["follow"] = False
        elif key in (ord("g"), curses.KEY_HOME):
            pager["scroll"], pager["follow"] = 0, False
        elif key in (ord("G"), curses.KEY_END):
            pager["scroll"] = len(self.pager_lines())
        elif key == ord("f"):
            pager["follow"] = not pager["follow"]
        elif key == ord("/"):
            self.start_search(pager["scroll"])
        elif key in (ord("n"), ord("N")) and self.query:
            self.pager_step(key == ord("n"))
            if pager["match"] is None:
                self.notify(f"no match for {self.query}", "yellow")
        elif key == 12:
            self.screen.clear()

    # ------------------------------------------------------------ key input

    def clamp_scroll(self, body: int) -> None:
        self.scroll = max(0, min(self.scroll, max(0, len(self.jobs) - body)))
        if self.index < self.scroll:
            self.scroll = self.index
        elif self.index >= self.scroll + body:
            self.scroll = self.index - body + 1

    def move(self, delta: int) -> None:
        if self.jobs:
            i = max(0, min(self.index + delta, len(self.jobs) - 1))
            self.selected_id = self.jobs[i]["id"]

    def handle_key(self, key: int) -> bool:
        """Returns True to quit."""
        if self.mode == "search":
            self.handle_search_key(key)
            return False
        if self.pager:
            self.handle_pager_key(key)
            return False
        if self.mode == "confirm":
            assert self.confirm is not None
            action = self.confirm[1]
            self.mode, self.confirm = None, None
            if key in (ord("y"), ord("Y")):
                self.guard(action)  # type: ignore[arg-type]
            else:
                self.notify("cancelled", "muted")
            return False
        if self.page:
            self.handle_job_key(key)
            return False
        return self.handle_dashboard_key(key)

    def handle_common_key(self, key: int, job: dict | None) -> bool:
        """Keys the job list and the job page share; True when handled."""
        if key == ord("?"):
            self.open_pager("keys", "static", self.help_lines())
        elif key == ord("D"):
            self.open_pager("scheduler log", "daemon", follow=True)
            self.reload()
        elif key == ord("e") and job:
            self.guard(
                lambda: self.notify(
                    f"{job['id']} "
                    + (
                        "enabled"
                        if hub(
                            "set_enabled",
                            job_id=job["id"],
                            enabled=not job.get("enabled"),
                        )["enabled"]
                        else "disabled"
                    ),
                    "yellow",
                )
            )
        elif key == ord("r") and job:
            self.ask(f"run {job['id']} now on {job['host']}?", "yellow", self.run_now)
        elif key == ord("x") and job:
            self.ask(
                f"stop the active run of {job['id']}?",
                "red",
                lambda: self.notify(
                    f"{job['id']}: stopping {hub('request_stop', job_id=job['id'])}",
                    "yellow",
                ),
            )
        elif key == 12:  # Ctrl-L
            self.screen.clear()
        else:
            return False
        return True

    def run_now(self) -> None:
        job = self.page_job if self.page else self.job
        assert job is not None
        run_id = hub("queue_run", job_id=job["id"])["run_id"]
        self.notify(f"{job['id']}: queued run {run_id} on {job['host']}", "cyan")
        if self.page:
            self.page["run_id"] = None  # follow the new run

    def handle_dashboard_key(self, key: int) -> bool:
        import curses

        job = self.job
        if self.handle_common_key(key, job):
            return False
        if key == ord("q"):
            return True
        if key == 27:
            if not self.query:
                return True
            self.query = ""
        elif key in (curses.KEY_DOWN, ord("j")):
            self.move(1)
        elif key in (curses.KEY_UP, ord("k")):
            self.move(-1)
        elif key in (curses.KEY_NPAGE, 4):
            self.move(self.body_rows(self.screen.getmaxyx()[0]))
        elif key in (curses.KEY_PPAGE, 21):
            self.move(-self.body_rows(self.screen.getmaxyx()[0]))
        elif key in (ord("g"), curses.KEY_HOME):
            self.move(-len(self.jobs))
        elif key in (ord("G"), curses.KEY_END):
            self.move(len(self.jobs))
        elif key in (10, 13, curses.KEY_ENTER, curses.KEY_RIGHT) and job:
            self.open_job(job["id"])
        elif key == ord("s"):
            self.sort = SORTS[(SORTS.index(self.sort) + 1) % len(SORTS)]
            self.apply_view()
        elif key == ord("/"):
            self.start_search(self.index)
        elif key in (ord("n"), ord("N")) and self.query:
            if not self.job_step(key == ord("n")):
                self.notify(f"no job matches {self.query}", "yellow")
        elif key == ord("d") and job:
            self.ask(
                f"delete {job['id']} and its run history?",
                "red",
                lambda: self.notify(
                    f"deleted {hub('delete_job', job_id=job['id'], keep_logs=False)['id']}",
                    "red",
                ),
            )
        return False

    def handle_job_key(self, key: int) -> None:
        import curses

        assert self.page is not None
        if self.handle_common_key(key, self.page_job):
            return
        run = self.page_run
        if key in (ord("q"), 27, curses.KEY_LEFT):
            self.page = None
        elif key in (curses.KEY_DOWN, ord("j")):
            self.move_run(1)
        elif key in (curses.KEY_UP, ord("k")):
            self.move_run(-1)
        elif key in (curses.KEY_NPAGE, 4):
            self.move_run(10)
        elif key in (curses.KEY_PPAGE, 21):
            self.move_run(-10)
        elif key in (ord("g"), curses.KEY_HOME):
            self.page["run_id"] = None
        elif key in (ord("G"), curses.KEY_END):
            self.move_run(len(self.page["runs"]))
        elif key in (10, 13, curses.KEY_ENTER, curses.KEY_RIGHT, ord("l")) and run:
            self.open_run_log(run)

    @staticmethod
    def help_lines() -> list[list[tuple[str, str]]]:
        lines: list[list[tuple[str, str]]] = []
        for section, keys in TUI_KEYS:
            lines += [[(section.upper(), "faint")]]
            lines += [[(f"  {k:<14}", "accent"), (d, "text")] for k, d in keys]
            lines.append([])
        return lines + [
            [("State and history come from the hub, same as the CLI.", "muted")]
        ]

    # ---------------------------------------------------------------- search

    def start_search(self, origin: int) -> None:
        """Open the "/" prompt; origin is the job index or log line it starts at."""
        self.mode, self.typed, self.origin = "search", "", origin

    def handle_search_key(self, key: int) -> None:
        import curses

        if key in (10, 13, curses.KEY_ENTER):
            self.mode, self.query = None, self.typed
            return
        if key == 27 or (key in (curses.KEY_BACKSPACE, 127, 8) and not self.typed):
            self.mode, self.query = None, ""
            if self.pager:
                self.pager["scroll"], self.pager["match"] = self.origin, None
            elif self.jobs:
                self.selected_id = self.jobs[min(self.origin, len(self.jobs) - 1)]["id"]
            return
        if key in (curses.KEY_BACKSPACE, 127, 8):
            self.typed = self.typed[:-1]
        elif 32 <= key < 127:
            self.typed += chr(key)
        else:
            return
        # incremental: the first match from where the search started
        if self.pager:
            self.pager["scroll"] = self.origin
            self.pager_step(True, origin=(self.origin, -1))
        elif self.jobs:
            self.selected_id = self.jobs[min(self.origin, len(self.jobs) - 1)]["id"]
            self.job_step(True, origin=self.origin - 1)

    def job_matches(self) -> list[int]:
        query = self.typed if self.mode == "search" else self.query
        return [
            i
            for i, job in enumerate(self.jobs)
            if search_ranges(self.haystack(job), query)
        ]

    def job_step(self, forward: bool, origin: int | None = None) -> bool:
        """n / N on the job list: select the next matching job, wrapping."""
        matches = self.job_matches()
        if not matches:
            return False
        i = step_to(matches, self.index if origin is None else origin, forward, int)
        self.selected_id = self.jobs[matches[i]]["id"]
        return True

    def ask(self, question: str, tone: str, action) -> None:
        self.mode, self.message = "confirm", None
        self.confirm = (question, action, tone)

    def guard(self, action) -> None:
        try:
            action()
        except KoyomiError as e:
            self.notify(str(e), "red")
        self.reload()


# ---------------------------------------------------------------- cli


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="koyomi",
        description="Small job scheduler for a few machines. State lives on the hub.",
        epilog="Command after '--' is shell-quoted; use --cmd for pipes/redirects.",
    )
    p.add_argument("--version", action="version", version=f"koyomi {VERSION}")
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")

    sp = sub.add_parser("init", help="set up this machine as a host")
    sp.add_argument("host", help="this machine's host name, e.g. mac or emanator")
    sp.add_argument("--hub-local", action="store_true", help="this machine is the hub")
    sp.add_argument("--hub-ssh", metavar="DEST", help="ssh destination of the hub")
    sp.add_argument(
        "--hub-koyomi", metavar="PATH", help="absolute path of koyomi on the hub"
    )
    sp.add_argument(
        "--always-on",
        action="store_true",
        help="a server: late or missed slots and downtime raise alerts",
    )
    sp.add_argument(
        "--smtp-env",
        required=True,
        metavar="FILE",
        help="dotenv file with SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD",
    )
    sp.add_argument("--email-from", required=True, metavar="ADDR")
    sp.add_argument("--email-to", required=True, metavar="ADDR")
    sp.set_defaults(func=cmd_init)

    def job_options(sp, adding: bool):
        g = sp.add_argument_group("schedule (pick one)")
        g.add_argument(
            "--cron", help="5-field cron in the job's time zone, e.g. '30 9 * * 1-5'"
        )
        g.add_argument("--every", help="fixed interval, e.g. 15m, 2h, 1d")
        g.add_argument("--at", help="one-time: 'YYYY-MM-DD HH:MM' or 'HH:MM'")
        g.add_argument(
            "--in", dest="in_", metavar="DURATION", help="one-time, relative: e.g. 10m"
        )
        sp.add_argument(
            "--host",
            help="host that runs the job"
            + (" (default: this machine)" if adding else ""),
        )
        sp.add_argument(
            "--tz",
            help="IANA time zone for --cron and --at"
            + (" (default: this machine's)" if adding else ""),
        )
        sp.add_argument("--cmd", help="shell command (run with /bin/sh -c)")
        sp.add_argument(
            "--cwd",
            help="working directory"
            + (" (default: current, for a job on this machine)" if adding else ""),
        )
        sp.add_argument(
            "--env",
            action="append",
            metavar="KEY=VALUE",
            help="extra env var (repeatable)",
        )
        sp.add_argument("--description", help="free-text note")
        sp.add_argument(
            "--catchup",
            choices=["once", "skip"],
            help="after downtime: run a missed slot once (default) or skip it",
        )
        sp.add_argument(
            "--timeout",
            help="kill the run after DURATION"
            + ("" if adding else " ('none' to clear)"),
        )

    sp = sub.add_parser("add", help="create a job")
    sp.add_argument("id")
    job_options(sp, adding=True)
    sp.add_argument("--disabled", action="store_true", help="create disabled")
    sp.set_defaults(func=cmd_add)

    sp = sub.add_parser("update", help="change a job (only given fields)")
    sp.add_argument("id")
    job_options(sp, adding=False)
    sp.add_argument("--unset-env", action="append", metavar="KEY")
    sp.set_defaults(func=cmd_update)

    sp = sub.add_parser("list", aliases=["ls"], help="list jobs")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("show", help="inspect a job")
    sp.add_argument("id")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_show)

    for name, flag in (("enable", True), ("disable", False)):
        sp = sub.add_parser(name, help=f"{name} a job")
        sp.add_argument("id")
        sp.set_defaults(func=lambda a, f=flag: cmd_enable(a, f))

    sp = sub.add_parser(
        "delete", aliases=["rm"], help="delete a job and its run history"
    )
    sp.add_argument("id")
    sp.add_argument("--keep-logs", action="store_true")
    sp.set_defaults(func=cmd_delete)

    sp = sub.add_parser(
        "run", help="run a job now on its host and stream its output (Ctrl-C stops it)"
    )
    sp.add_argument("id")
    sp.add_argument(
        "--detach", "-d", action="store_true", help="don't follow the output"
    )
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("stop", help="stop a job's active run")
    sp.add_argument("id")
    sp.set_defaults(func=cmd_stop)

    sp = sub.add_parser("history", help="past runs (all jobs or one)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("-n", "--limit", type=int, default=20)
    sp.add_argument(
        "--failed",
        action="store_true",
        help="only failed, timed out, interrupted or skipped runs",
    )
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("logs", help="output of a job's latest run (or scheduler log)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("--run", help="specific run id")
    sp.add_argument(
        "-n", "--lines", type=int, default=100, help="tail N lines (0 = all)"
    )
    sp.add_argument("--daemon", action="store_true", help="the hub's scheduler log")
    sp.set_defaults(func=cmd_logs)

    sp = sub.add_parser("status", help="hosts, scheduler health, failing jobs")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "tui", aliases=["ui"], help="interactive job dashboard and controls"
    )
    sp.add_argument(
        "--refresh",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help="data refresh interval, 0.5–60 seconds (default: 2)",
    )
    sp.set_defaults(func=cmd_tui)

    sp = sub.add_parser(
        "service", help="manage the scheduler service (launchd/systemd)"
    )
    sp.add_argument(
        "action", choices=["install", "uninstall", "start", "stop", "restart", "status"]
    )
    sp.set_defaults(func=cmd_service)

    sp = sub.add_parser(
        "daemon", help="run the scheduler in the foreground (the service uses this)"
    )
    sp.set_defaults(func=lambda a: daemon_main())

    sp = sub.add_parser("_exec")  # internal: runner for a claimed run
    sp.add_argument("id")
    sp.add_argument("run_id")
    sp.set_defaults(func=lambda a: execute(a.id, a.run_id))

    sp = sub.add_parser("_rpc")  # internal: serve one hub operation over stdin/stdout
    sp.set_defaults(func=lambda a: rpc_main())
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    command_argv: list[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, command_argv = argv[:i], argv[i + 1 :]
    args = build_parser().parse_args(argv)
    args.argv_command = command_argv
    if command_argv and args.command not in ("add", "update"):
        print(
            "koyomi: error: '--' command is only valid for add/update", file=sys.stderr
        )
        return 2
    try:
        return args.func(args) or 0
    except KoyomiError as e:
        print(f"koyomi: error: {e}", file=sys.stderr)
        return 1
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    sys.exit(main())
