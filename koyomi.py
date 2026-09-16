#!/usr/bin/env python3
"""Koyomi: a tiny local job scheduler.

All state is plain JSON under ~/.koyomi/ (override with KOYOMI_HOME):

  jobs/<id>.json               job definition + scheduling state
  runs/<id>/<run_id>.json      one record per run (status, exit code, timing)
  runs/<id>/<run_id>.log       combined stdout/stderr of that run
  daemon.json                  scheduler heartbeat (pid, last tick)
  daemon.log                   scheduler event log

The scheduler (`koyomi daemon`) is started by launchd. Each tick it claims due
jobs by persisting the advanced next_run *before* spawning the command, so a
slot is never executed twice (at-most-once), and missed slots collapse into a
single catch-up run after sleep or shutdown.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
import plistlib
import re
import shlex
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

VERSION = "0.1.0"
LABEL = "local.koyomi.scheduler"
TICK_SECONDS = 15  # max sleep between scheduler checks
GRACE_SECONDS = 300  # a run later than this counts as "missed"
KEEP_RUNS = 50  # run records kept per job
MIN_INTERVAL = 60
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class KoyomiError(Exception):
    pass


# ---------------------------------------------------------------- paths & io


def home() -> Path:
    return Path(os.environ.get("KOYOMI_HOME", "~/.koyomi")).expanduser()


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


def plist_path() -> Path:
    return Path("~/Library/LaunchAgents").expanduser() / f"{LABEL}.plist"


def read_json(path: Path):
    with open(path) as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    """Atomic write: temp file + fsync + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@contextlib.contextmanager
def store_lock():
    """Global lock around every read-modify-write of job files."""
    home().mkdir(parents=True, exist_ok=True)
    with open(home() / "koyomi.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


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


def load_runs(job_id: str) -> list[dict]:
    runs = []
    for path in sorted(runs_dir(job_id).glob("*.json")):
        with contextlib.suppress(OSError, ValueError):
            runs.append(read_json(path))
    return runs


# ---------------------------------------------------------------- time


def now() -> dt.datetime:
    return dt.datetime.now().astimezone()


def iso(t: dt.datetime | None) -> str | None:
    return t.isoformat(timespec="seconds") if t else None


def parse_iso(s: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(s) if s else None


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


def parse_when(text: str) -> dt.datetime:
    """'HH:MM' (next occurrence) or ISO-ish 'YYYY-MM-DD HH:MM[:SS][+offset]'."""
    v = text.strip()
    if re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", v):
        parts = [int(x) for x in v.split(":")]
        cur = now()
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
    return t.astimezone() if t.tzinfo is None else t


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
    """Standard 5-field cron (minute hour day-of-month month day-of-week), local time."""

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

    def next_after(self, after: dt.datetime) -> dt.datetime:
        start = after.astimezone().replace(
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
                        aware = cand.astimezone()
                        if aware > after:
                            return aware
            day += dt.timedelta(days=1)
        raise KoyomiError("cron expression never matches")


# ---------------------------------------------------------------- scheduling


def describe_schedule(s: dict) -> str:
    if "cron" in s:
        return f"cron {s['cron']}"
    if "every" in s:
        return f"every {s['every']}"
    return f"once at {fmt_time(s['at'])}"


def compute_next(job: dict, after: dt.datetime) -> dt.datetime | None:
    s = job["schedule"]
    if "cron" in s:
        return Cron(s["cron"]).next_after(after)
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


def boot_time() -> dt.datetime | None:
    global _BOOT_TIME
    if _BOOT_TIME is None:
        try:
            out = subprocess.run(
                ["sysctl", "-n", "kern.boottime"], capture_output=True, text=True
            ).stdout
            m = re.search(r"sec = (\d+)", out)
            _BOOT_TIME = (
                dt.datetime.fromtimestamp(int(m.group(1))).astimezone() if m else False
            )
        except OSError:
            _BOOT_TIME = False
    return _BOOT_TIME or None


_BOOT_TIME = None


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


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
    run_id = cur.strftime("%Y%m%dT%H%M%S") + "-" + os.urandom(2).hex()
    return {
        "run_id": run_id,
        "job_id": job["id"],
        "trigger": trigger,
        "status": "running",
        "scheduled_for": iso(scheduled_for),
        "started_at": iso(cur),
        "finished_at": None,
        "duration_seconds": None,
        "exit_code": None,
        "error": None,
        "command": job["command"],
        "cwd": job["cwd"],
        "log": str(run_log_path(job["id"], run_id)),
    }


def claim_run(job: dict, trigger: str, cur: dt.datetime, scheduled_for=None) -> dict:
    """Create a 'running' record and mark the job as running (caller holds lock and saves)."""
    rec = new_run_record(job, trigger, cur, scheduled_for)
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    job["running"] = {
        "run_id": rec["run_id"],
        "pid": None,
        "started_at": rec["started_at"],
        "trigger": trigger,
    }
    return rec


def record_skip(
    job: dict, reason: str, scheduled_for: dt.datetime, cur: dt.datetime
) -> None:
    rec = new_run_record(job, "schedule", cur, scheduled_for)
    rec.update(status="skipped", finished_at=iso(cur), error=reason)
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    if not job.get("running"):
        job["last_run"] = run_summary(rec)
    log_event(f"{job['id']}: skipped slot {iso(scheduled_for)}: {reason}")


def reconcile_running(job: dict) -> bool:
    """Clear a 'running' marker whose runner process is gone (crash, reboot, kill)."""
    r = job.get("running")
    if not r:
        return False
    started = parse_iso(r["started_at"])
    assert started is not None
    pid = r.get("pid")
    if pid is None:
        if (now() - started).total_seconds() < 60:
            return False  # just claimed, spawn in progress
        alive = False
    else:
        bt = boot_time()
        alive = pid_alive(pid) and not (bt and started < bt)
    if alive:
        return False
    path = run_json_path(job["id"], r["run_id"])
    try:
        rec = read_json(path)
    except (OSError, ValueError):
        rec = None
    if rec and rec.get("status") == "running":
        rec.update(
            status="interrupted",
            finished_at=iso(now()),
            error="runner disappeared (reboot, crash or kill) before finishing",
        )
        write_json(path, rec)
    if rec:
        job["last_run"] = run_summary(rec)
    job["running"] = None
    log_event(
        f"{job['id']}: run {r['run_id']} marked interrupted (runner pid {pid} gone)"
    )
    return True


def spawn_runner(job_id: str, run_id: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "_exec", job_id, run_id],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def tick(cur: dt.datetime | None = None):
    """One scheduler pass. Returns (spawned runner processes, earliest upcoming next_run)."""
    cur = cur or now()
    launched: list[subprocess.Popen] = []
    earliest = None
    with store_lock():
        for job in load_jobs():
            path = job_path(job["id"])
            changed = reconcile_running(job)
            due = parse_iso(job.get("next_run"))
            if job.get("enabled") and due and due <= cur:
                # Claim the slot first: persist the advanced next_run before running anything.
                job["next_run"] = iso(compute_next(job, cur))
                late = (cur - due).total_seconds()
                if job.get("running"):
                    record_skip(job, "previous run still in progress", due, cur)
                    write_json(path, job)
                elif late > GRACE_SECONDS and job.get("catchup") == "skip":
                    record_skip(
                        job, f"missed by {fmt_dur(late)} (catchup=skip)", due, cur
                    )
                    write_json(path, job)
                else:
                    rec = claim_run(job, "schedule", cur, due)
                    write_json(path, job)
                    if late > GRACE_SECONDS:
                        log_event(
                            f"{job['id']}: catch-up run for missed slot {iso(due)} "
                            f"({fmt_dur(late)} late)"
                        )
                    log_event(f"{job['id']}: starting run {rec['run_id']}")
                    try:
                        proc = spawn_runner(job["id"], rec["run_id"])
                        job["running"]["pid"] = proc.pid
                        launched.append(proc)
                    except OSError as e:
                        rec.update(
                            status="failed",
                            finished_at=iso(now()),
                            error=f"could not spawn runner: {e}",
                        )
                        write_json(run_json_path(job["id"], rec["run_id"]), rec)
                        job["running"] = None
                        job["last_run"] = run_summary(rec)
                        log_event(f"{job['id']}: {rec['error']}")
                    write_json(path, job)
            elif changed:
                write_json(path, job)
            nxt = parse_iso(job.get("next_run"))
            if job.get("enabled") and nxt and (earliest is None or nxt < earliest):
                earliest = nxt
    return launched, earliest


# ---------------------------------------------------------------- execution


def notify(title: str, message: str) -> None:
    script = [
        "-e",
        "on run argv",
        "-e",
        "display notification (item 2 of argv) with title (item 1 of argv)",
        "-e",
        "end run",
    ]
    with contextlib.suppress(OSError):
        subprocess.run(
            ["osascript", *script, title, message],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )


def kill_group(proc: subprocess.Popen) -> None:
    for sig, wait in ((signal.SIGTERM, 10), (signal.SIGKILL, None)):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, sig)
        try:
            proc.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def tail_bytes(path: Path, n: int = 2000) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - n))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


def prune_runs(job_id: str) -> None:
    records = sorted(runs_dir(job_id).glob("*.json"))
    for path in records[:-KEEP_RUNS]:
        with contextlib.suppress(OSError, ValueError):
            if read_json(path).get("status") == "running":
                continue
            path.unlink()
            path.with_suffix(".log").unlink(missing_ok=True)


def execute(job_id: str, run_id: str, echo: bool = False) -> int:
    """Run a claimed job, capture output, write the final record. Returns an exit status."""
    job = load_job(job_id)
    rec_path, log_path = run_json_path(job_id, run_id), run_log_path(job_id, run_id)
    rec = read_json(rec_path)
    env = {
        **os.environ,
        **(job.get("env") or {}),
        "KOYOMI_JOB": job_id,
        "KOYOMI_RUN_ID": run_id,
    }
    timeout = parse_duration(job["timeout"]) if job.get("timeout") else None
    status, exit_code, error = "failed", None, None
    started = time.time()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as log:
        try:
            proc = subprocess.Popen(
                ["/bin/sh", "-c", job["command"]],
                cwd=job["cwd"],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as e:
            error = f"could not start command: {e}"
            log.write(f"koyomi: {error}\n".encode())
        else:
            try:
                exit_code = wait_for(proc, log_path, timeout, echo)
                if exit_code == 0:
                    status = "success"
                elif exit_code < 0:
                    error = f"killed by signal {-exit_code}"
                else:
                    error = f"exit code {exit_code}"
            except subprocess.TimeoutExpired:
                kill_group(proc)
                status, exit_code, error = (
                    "timeout",
                    proc.returncode,
                    f"timed out after {job['timeout']}",
                )
            except KeyboardInterrupt:
                kill_group(proc)
                status, exit_code, error = (
                    "interrupted",
                    proc.returncode,
                    "interrupted by user",
                )
    if echo:
        wait_for_echo_flush(log_path)
    rec.update(
        status=status,
        exit_code=exit_code,
        error=error,
        finished_at=iso(now()),
        duration_seconds=round(time.time() - started, 3),
    )
    if status != "success":
        rec["output_tail"] = tail_bytes(log_path)
    with store_lock():
        if not job_path(job_id).exists():
            return 0 if status == "success" else 1  # job deleted mid-run
        write_json(rec_path, rec)
        job = load_job(job_id)
        if (job.get("running") or {}).get("run_id") == run_id:
            job["running"] = None
        job["last_run"] = run_summary(rec)
        write_json(job_path(job_id), job)
    log_event(f"{job_id}: run {run_id} {status}" + (f" ({error})" if error else ""))
    if status != "success" and job.get("notify"):
        notify(f"Koyomi: {job_id} {status}", error or status)
    prune_runs(job_id)
    if status == "success":
        return 0
    return exit_code if exit_code and exit_code > 0 else 1


_echo_pos = 0


def _echo(log_path: Path) -> None:
    global _echo_pos
    with open(log_path, "rb") as f:
        f.seek(_echo_pos)
        data = f.read()
    if data:
        _echo_pos += len(data)
        sys.stdout.buffer.write(data)
        sys.stdout.flush()


def wait_for_echo_flush(log_path: Path) -> None:
    with contextlib.suppress(OSError):
        _echo(log_path)


def wait_for(
    proc: subprocess.Popen, log_path: Path, timeout: int | None, echo: bool
) -> int:
    if not echo:
        return proc.wait(timeout=timeout)
    deadline = time.time() + timeout if timeout else None
    while True:
        rc = proc.poll()
        _echo(log_path)
        if rc is not None:
            return rc
        if deadline and time.time() > deadline:
            raise subprocess.TimeoutExpired(proc.args, timeout or 0)
        time.sleep(0.2)


# ---------------------------------------------------------------- daemon


def daemon_main() -> int:
    home().mkdir(parents=True, exist_ok=True)
    lock = open(home() / "daemon.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("koyomi: scheduler already running", file=sys.stderr)
        return 1
    boot_time()  # cache before ignoring SIGCHLD
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
    signal.signal(signal.SIGINT, lambda *_: stop.append(1))
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)  # auto-reap runner processes
    started = now()
    log_event(f"scheduler started (pid {os.getpid()}, version {VERSION})")
    last = started
    while not stop:
        cur = now()
        gap = (cur - last).total_seconds()
        if gap > TICK_SECONDS + 60:
            log_event(
                f"clock jumped {fmt_dur(gap)} since last tick (sleep/suspend); checking missed jobs"
            )
        last = cur
        earliest = None
        try:
            _, earliest = tick(cur)
        except Exception:
            log_event("tick error:\n" + traceback.format_exc())
        with contextlib.suppress(OSError):
            write_json(
                home() / "daemon.json",
                {
                    "pid": os.getpid(),
                    "version": VERSION,
                    "started_at": iso(started),
                    "last_tick": iso(cur),
                },
            )
        wake = cur + dt.timedelta(seconds=TICK_SECONDS)
        if earliest and earliest < wake:
            wake = earliest
        # Poll the wall clock so we notice wake-from-sleep within a second.
        while not stop and now() < wake:
            time.sleep(1)
    log_event("scheduler stopped")
    return 0


def launchctl(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["launchctl", *args], capture_output=True, text=True)


def service_loaded() -> bool:
    return launchctl("print", f"gui/{os.getuid()}/{LABEL}").returncode == 0


def daemon_state() -> tuple[bool, dict | None]:
    try:
        info = read_json(home() / "daemon.json")
    except (OSError, ValueError):
        return False, None
    return pid_alive(info["pid"]), info


def cmd_service(args) -> int:
    domain, target = f"gui/{os.getuid()}", f"gui/{os.getuid()}/{LABEL}"
    action = args.action
    alive, info = daemon_state()
    old_pid = info["pid"] if alive and info else None
    if action == "install":
        home().mkdir(parents=True, exist_ok=True)
        plist = {
            "Label": LABEL,
            "ProgramArguments": [
                sys.executable,
                str(Path(__file__).resolve()),
                "daemon",
            ],
            "RunAtLoad": True,
            "KeepAlive": True,
            "EnvironmentVariables": {
                "PATH": ":".join(
                    dict.fromkeys(os.environ.get("PATH", "/usr/bin:/bin").split(":"))
                ),
                "KOYOMI_HOME": str(home()),
            },
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
        print(
            "scheduler stopped and launchd agent removed (jobs in ~/.koyomi are kept)"
        )
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
    return 0


def wait_for_daemon(previous_pid=None, seconds: float = 5) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        alive, info = daemon_state()
        if alive and info and info["pid"] != previous_pid:
            return
        time.sleep(0.2)


def print_daemon_status() -> None:
    alive, info = daemon_state()
    loaded = service_loaded()
    if alive and info:
        print(
            f"scheduler: running (pid {info['pid']}, last tick {fmt_rel(info['last_tick'])})"
        )
    else:
        print("scheduler: NOT running")
    print(
        f"launchd:   {'loaded' if loaded else 'not loaded'} ({LABEL})"
        + (
            ""
            if plist_path().exists()
            else " - agent not installed, run: koyomi service install"
        )
    )
    print(f"home:      {home()}")


# ---------------------------------------------------------------- commands


def schedule_from_args(args, required: bool) -> dict | None:
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
        return {"at": iso(parse_when(val))}
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


def refresh_next_run(job: dict) -> None:
    if not job["enabled"]:
        job["next_run"] = None
        return
    job["next_run"] = iso(compute_next(job, now()))
    if job["next_run"] is None and "at" in job["schedule"]:
        raise KoyomiError(f"time {fmt_time(job['schedule']['at'])} is in the past")


def cmd_add(args) -> int:
    if not ID_RE.match(args.id):
        raise KoyomiError(
            "job id must be letters, digits, '.', '_' or '-' (max 64 chars)"
        )
    command = resolve_command(args)
    if not command:
        raise KoyomiError(
            "a command is required: --cmd 'shell command' or -- program args"
        )
    cwd = str(Path(args.cwd or os.getcwd()).expanduser().resolve())
    if not Path(cwd).is_dir():
        raise KoyomiError(f"working directory does not exist: {cwd}")
    if args.timeout:
        parse_duration(args.timeout)
    ts = iso(now())
    job = {
        "id": args.id,
        "description": args.description or "",
        "command": command,
        "cwd": cwd,
        "env": parse_env(args.env),
        "schedule": schedule_from_args(args, required=True),
        "enabled": not args.disabled,
        "catchup": args.catchup or "once",
        "timeout": args.timeout,
        "notify": bool(args.notify),
        "created_at": ts,
        "updated_at": ts,
        "next_run": None,
        "last_run": None,
        "running": None,
    }
    refresh_next_run(job)
    with store_lock():
        if job_path(args.id).exists():
            raise KoyomiError(f"job already exists: {args.id} (use: koyomi update)")
        write_json(job_path(args.id), job)
    print(
        f"added {args.id}: {describe_schedule(job['schedule'])}; "
        f"next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}".rstrip()
    )
    warn_if_daemon_down()
    return 0


def cmd_update(args) -> int:
    with store_lock():
        job = load_job(args.id)
        reschedule = False
        command = resolve_command(args)
        if command:
            job["command"] = command
        if args.cwd:
            cwd = str(Path(args.cwd).expanduser().resolve())
            if not Path(cwd).is_dir():
                raise KoyomiError(f"working directory does not exist: {cwd}")
            job["cwd"] = cwd
        if args.description is not None:
            job["description"] = args.description
        sched = schedule_from_args(args, required=False)
        if sched:
            job["schedule"], reschedule = sched, True
        if args.catchup:
            job["catchup"] = args.catchup
        if args.timeout:
            job["timeout"] = (
                None if args.timeout.lower() in ("none", "0") else args.timeout
            )
            if job["timeout"]:
                parse_duration(job["timeout"])
        if args.notify is not None:
            job["notify"] = args.notify
        job["env"] = {**job.get("env", {}), **parse_env(args.env)}
        for key in args.unset_env or []:
            job["env"].pop(key, None)
        if reschedule:
            refresh_next_run(job)
        job["updated_at"] = iso(now())
        write_json(job_path(args.id), job)
    print(
        f"updated {args.id}; next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}".rstrip()
    )
    return 0


def set_job_enabled(job_id: str, enabled: bool) -> dict:
    """Change a job's scheduling state without affecting an active run."""
    with store_lock():
        job = load_job(job_id)
        job["enabled"] = enabled
        refresh_next_run(job)  # re-enabling never back-fills the disabled period
        job["updated_at"] = iso(now())
        write_json(job_path(job_id), job)
    return job


def cmd_enable(args, enabled: bool) -> int:
    job = set_job_enabled(args.id, enabled)
    if enabled:
        print(
            f"enabled {args.id}; next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}"
        )
        warn_if_daemon_down()
    else:
        print(f"disabled {args.id}")
    return 0


def cmd_delete(args) -> int:
    job = delete_job(args.id, args.keep_logs)
    if job.get("running"):
        print(
            f"note: run {job['running']['run_id']} (pid {job['running']['pid']}) is still in progress",
            file=sys.stderr,
        )
    print(f"deleted {args.id}" + (" (run history kept)" if args.keep_logs else ""))
    return 0


def start_detached_run(job_id: str) -> dict:
    """Claim and start a manual runner.  The claim is persisted before spawn."""
    with store_lock():
        job = load_job(job_id)
        reconcile_running(job)
        if job.get("running"):
            r = job["running"]
            raise KoyomiError(
                f"{job_id} is already running (run {r['run_id']}, pid {r['pid']})"
            )
        rec = claim_run(job, "manual", now())
        write_json(job_path(job_id), job)
        try:
            proc = spawn_runner(job_id, rec["run_id"])
        except OSError as e:
            rec.update(
                status="failed",
                finished_at=iso(now()),
                error=f"could not spawn runner: {e}",
            )
            write_json(run_json_path(job_id, rec["run_id"]), rec)
            job["running"] = None
            job["last_run"] = run_summary(rec)
            write_json(job_path(job_id), job)
            raise KoyomiError(rec["error"]) from None
        job["running"]["pid"] = proc.pid
        write_json(job_path(job_id), job)
    log_event(f"{job_id}: manual run {rec['run_id']} (detached)")
    return rec


def stop_run(job_id: str) -> str:
    """Interrupt the active run; the runner itself writes the final record."""
    r = load_job(job_id).get("running")
    if not r:
        raise KoyomiError(f"{job_id} is not running")
    if r.get("pid") is None:
        raise KoyomiError(f"{job_id} is still starting; try again in a moment")
    try:
        os.kill(r["pid"], signal.SIGINT)
    except ProcessLookupError:
        raise KoyomiError(f"run {r['run_id']} has already exited") from None
    except PermissionError as e:
        raise KoyomiError(f"cannot signal pid {r['pid']}: {e}") from None
    return r["run_id"]


def cmd_run(args) -> int:
    if args.detach:
        rec = start_detached_run(args.id)
        print(f"started {args.id} run {rec['run_id']}; log: {rec['log']}")
        return 0
    with store_lock():
        job = load_job(args.id)
        reconcile_running(job)
        if job.get("running"):
            r = job["running"]
            raise KoyomiError(
                f"{args.id} is already running (run {r['run_id']}, pid {r['pid']})"
            )
        rec = claim_run(job, "manual", now())
        job["running"]["pid"] = os.getpid()
        write_json(job_path(args.id), job)
    log_event(f"{args.id}: manual run {rec['run_id']}")
    code = execute(args.id, rec["run_id"], echo=True)
    final = (
        read_json(run_json_path(args.id, rec["run_id"]))
        if job_path(args.id).exists()
        else rec
    )
    print(
        f"--- koyomi: {args.id} {final['status']} "
        f"(exit {final['exit_code']}, {fmt_dur(final['duration_seconds'])}) run {rec['run_id']}",
        file=sys.stderr,
    )
    return code


def wait_until_idle(job_id: str, seconds: float = 10) -> dict | None:
    """Poll until the runner has cleared its 'running' marker. Returns the final run."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        job = load_job(job_id)
        if not job.get("running"):
            return job.get("last_run")
        time.sleep(0.1)
    return None


def cmd_stop(args) -> int:
    run_id = stop_run(args.id)
    last = wait_until_idle(args.id)
    if last is None:
        print(f"signalled {args.id} run {run_id}; still shutting down")
        return 0
    print(
        f"stopped {args.id} run {run_id}: {last['status']}"
        + (f" ({last['error']})" if last.get("error") else "")
    )
    return 0


def job_status_word(job: dict) -> str:
    if job.get("running"):
        return "running"
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
    jobs = load_jobs()
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
                job_status_word(j),
                describe_schedule(j["schedule"]),
                fmt_time(j.get("next_run")),
                f"{last.get('status', '-')} {fmt_rel(last.get('finished_at') or last.get('started_at'))}".strip(),
            ]
        )
    print_table(rows, ["ID", "STATE", "SCHEDULE", "NEXT RUN", "LAST RUN"])
    return 0


def cmd_show(args) -> int:
    job = load_job(args.id)
    if args.json:
        print(json.dumps(job, indent=2))
        return 0
    last = job.get("last_run")
    print(f"id:          {job['id']}")
    if job.get("description"):
        print(f"description: {job['description']}")
    print(f"state:       {job_status_word(job)}")
    print(f"command:     {job['command']}")
    print(f"cwd:         {job['cwd']}")
    if job.get("env"):
        print(f"env:         {' '.join(f'{k}={v}' for k, v in job['env'].items())}")
    print(f"schedule:    {describe_schedule(job['schedule'])}")
    print(
        f"catchup:     {job['catchup']}   timeout: {job.get('timeout') or 'none'}   "
        f"notify: {'on failure' if job.get('notify') else 'off'}"
    )
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
        print(
            f"running:     run {r['run_id']} pid {r['pid']} since {fmt_time(r['started_at'])}"
        )
    if last:
        print(
            f"last run:    {last['status']} exit={last['exit_code']} at {fmt_time(last['started_at'])} "
            f"({last['trigger']}) run {last['run_id']}"
        )
        if last.get("error"):
            print(f"last error:  {last['error']}")
    print(f"file:        {job_path(job['id'])}")
    return 0


def cmd_history(args) -> int:
    ids = [args.id] if args.id else [j["id"] for j in load_jobs()]
    if args.id:
        load_job(args.id)
    runs = [r for i in ids for r in load_runs(i)]
    runs.sort(key=lambda r: r["started_at"])
    if args.failed:
        runs = [r for r in runs if r["status"] not in ("success", "running")]
    runs = runs[-args.limit :]
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
        ["RUN", "JOB", "TRIGGER", "STATUS", "EXIT", "STARTED", "DURATION", "ERROR"],
    )
    return 0


def tail_lines(path: Path, n: int) -> str:
    try:
        lines = path.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        raise KoyomiError(f"log not found: {path}") from None
    return "\n".join(lines[-n:]) if n else "\n".join(lines)


def cmd_logs(args) -> int:
    if args.daemon or not args.id:
        path = home() / "daemon.log"
        print(f"==> {path}", file=sys.stderr)
        print(tail_lines(path, args.lines))
        return 0
    load_job(args.id)
    if args.run:
        run_id = args.run
    else:
        runs = [r for r in load_runs(args.id) if r["status"] != "skipped"]
        if not runs:
            raise KoyomiError(f"{args.id} has no runs yet")
        run_id = runs[-1]["run_id"]
    rec = (
        read_json(run_json_path(args.id, run_id))
        if run_json_path(args.id, run_id).exists()
        else {}
    )
    print(
        f"==> run {run_id}: {rec.get('status', '?')} exit={rec.get('exit_code')} "
        f"started {fmt_time(rec.get('started_at'))}",
        file=sys.stderr,
    )
    print(tail_lines(run_log_path(args.id, run_id), args.lines))
    return 0


def cmd_status(args) -> int:
    print_daemon_status()
    jobs = load_jobs()
    enabled = [j for j in jobs if j.get("enabled")]
    running = [j for j in jobs if j.get("running")]
    print(
        f"jobs:      {len(jobs)} total, {len(enabled)} enabled, {len(running)} running"
    )
    upcoming = sorted(
        (j for j in enabled if j.get("next_run")), key=lambda j: j["next_run"]
    )
    if upcoming:
        j = upcoming[0]
        print(
            f"next:      {j['id']} at {fmt_time(j['next_run'])} ({fmt_rel(j['next_run'])})"
        )
    failing = [
        j
        for j in jobs
        if (j.get("last_run") or {}).get("status")
        in ("failed", "timeout", "interrupted")
    ]
    if failing:
        print("failing:")
        for j in failing:
            lr = j["last_run"]
            print(
                f"  {j['id']}: {lr['status']} ({lr.get('error')}) at {fmt_time(lr['started_at'])}"
                f" -> koyomi logs {j['id']}"
            )
    return 0


def warn_if_daemon_down() -> None:
    alive, _ = daemon_state()
    if not alive:
        print(
            "warning: scheduler is not running; start it with: koyomi service start",
            file=sys.stderr,
        )


# ---------------------------------------------------------------- terminal UI

FAILURE_STATES = {"failed", "timeout", "interrupted"}
STATE_COLORS = {
    "running": "cyan",
    "stale": "yellow",
    "disabled": "dim",
    "done": "dim",
    "enabled": "green",
}
RUN_COLORS = {
    "success": "green",
    "failed": "red",
    "timeout": "red",
    "interrupted": "red",
    "skipped": "yellow",
    "running": "cyan",
}


def dashboard_snapshot() -> tuple[bool, dict | None, list[dict]]:
    """Read the dashboard model. No state is modified by the UI refresh."""
    alive, daemon = daemon_state()
    return alive, daemon, load_jobs()


def dashboard_status(job: dict) -> str:
    if job.get("running"):
        pid = job["running"].get("pid")
        return "running" if pid is None or pid_alive(pid) else "stale"
    return job_status_word(job)


def tui_tail(path: Path, lines: int = 200) -> list[str]:
    return tail_bytes(path, 64_000).splitlines()[-lines:]


def job_log_path(job: dict) -> Path | None:
    """Log of the active run, else of the most recent one."""
    ref = job.get("running") or job.get("last_run")
    if not ref:
        return None
    return Path(ref.get("log") or run_log_path(job["id"], ref["run_id"]))


def delete_job(job_id: str, keep_logs: bool = False) -> dict:
    import shutil

    with store_lock():
        job = load_job(job_id)
        job_path(job_id).unlink()
        if not keep_logs:
            shutil.rmtree(runs_dir(job_id), ignore_errors=True)
    return job


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
    if not 0.2 <= refresh <= 60:
        raise KoyomiError("--refresh must be between 0.2 and 60 seconds")
    os.environ.setdefault("ESCDELAY", "25")
    return curses.wrapper(lambda screen: TuiApp(screen, refresh).run())


# (header, key, width); 0 sizes to content, -1 absorbs the leftover width
TUI_COLUMNS = [
    ("JOB", "id", 0),
    ("STATE", "state", 8),
    ("SCHEDULE", "schedule", -1),
    ("NEXT", "next", 13),
    ("LAST", "last", 11),
    ("WHEN", "when", 12),
]
TUI_KEYS = [
    ("j / k, ↑ ↓", "move selection"),
    ("g / G", "first / last job"),
    ("PgUp / PgDn", "page up / down"),
    ("Enter", "toggle the detail pane"),
    ("e", "enable / disable (an active run keeps going)"),
    ("r", "run now, in the background"),
    ("x", "stop the active run"),
    ("d", "delete the job and its history (asks first)"),
    ("l", "log of the latest run (live)"),
    ("h", "run history"),
    ("D", "scheduler log (live)"),
    ("/", "filter jobs; Esc clears the filter"),
    ("s", "sort by id / next run / state"),
    ("Ctrl-L", "redraw"),
    ("q", "quit, or close what is open"),
]
SORTS = ("id", "next", "state")


class TuiApp:
    """Small curses dashboard: everything reads the same JSON files as the CLI."""

    def __init__(self, screen, refresh: float):
        self.screen, self.refresh = screen, refresh
        self.alive, self.daemon = False, None
        self.all_jobs: list[dict] = []
        self.jobs: list[dict] = []
        self.selected_id: str | None = None
        self.scroll = 0
        self.expanded = False
        self.sort = "id"
        self.filter = ""
        self.mode: str | None = None  # None | "filter" | "confirm"
        self.confirm: tuple[str, object] | None = None
        self.message: tuple[str, str, float] | None = None
        self.pager: dict | None = None
        self.colors: dict[str, int] = {}

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
        dirty = True
        while True:
            if dirty:
                self.draw()
                dirty = False
            key = self.screen.getch()
            if key != -1:
                if self.handle_key(key):
                    return 0
                dirty = True
            if time.monotonic() >= next_load:
                next_load = time.monotonic() + self.refresh
                self.reload()
                dirty = True

    def init_colors(self) -> None:
        import curses

        with contextlib.suppress(curses.error):
            curses.start_color()
            curses.use_default_colors()
            if not curses.has_colors():
                return
            for i, (name, fg) in enumerate(
                (
                    ("green", curses.COLOR_GREEN),
                    ("red", curses.COLOR_RED),
                    ("yellow", curses.COLOR_YELLOW),
                    ("cyan", curses.COLOR_CYAN),
                    ("blue", curses.COLOR_BLUE),
                ),
                1,
            ):
                curses.init_pair(i, fg, -1)
                self.colors[name] = curses.color_pair(i)
        self.colors["dim"] = curses.A_DIM

    def color(self, name: str | None) -> int:
        return self.colors.get(name or "", 0)

    def reload(self) -> None:
        try:
            self.alive, self.daemon, self.all_jobs = dashboard_snapshot()
        except Exception as e:  # a half-written file should not kill the UI
            self.notify(f"refresh failed: {e}", "red")
            return
        self.apply_view()
        if self.pager:
            self.pager["lines"] = self.pager["source"]()

    def apply_view(self) -> None:
        """Re-derive the visible job list, keeping the selection where it can."""
        jobs = self.all_jobs
        if self.filter:
            needle = self.filter.lower()
            jobs = [j for j in jobs if needle in self.haystack(j)]
        keys = {
            "id": lambda j: j["id"],
            "next": lambda j: (j.get("next_run") is None, j.get("next_run") or ""),
            "state": lambda j: (
                list(STATE_COLORS).index(dashboard_status(j))
                if dashboard_status(j) in STATE_COLORS
                else 9,
                j["id"],
            ),
        }
        self.jobs = sorted(jobs, key=keys[self.sort])
        if not self.jobs:
            self.selected_id = None
        elif self.selected_id not in {j["id"] for j in self.jobs}:
            self.selected_id = self.jobs[min(self.index, len(self.jobs) - 1)]["id"]

    @staticmethod
    def haystack(job: dict) -> str:
        return " ".join(
            (job["id"], job.get("description") or "", job["command"])
        ).lower()

    @property
    def index(self) -> int:
        for i, job in enumerate(self.jobs):
            if job["id"] == self.selected_id:
                return i
        return 0

    @property
    def job(self) -> dict | None:
        return self.jobs[self.index] if self.jobs else None

    def notify(self, text: str, color: str = "") -> None:
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

    def draw(self) -> None:
        self.screen.erase()
        if self.pager:
            self.draw_pager()
        else:
            self.draw_dashboard()
        self.screen.refresh()

    def draw_dashboard(self) -> None:
        import curses

        height, width = self.screen.getmaxyx()
        self.draw_header(width)
        columns = self.layout(width)
        body_top = 4
        detail = self.detail_lines() if self.expanded and self.job else []
        body = max(1, height - body_top - 2 - (len(detail) + 1 if detail else 0))
        self.clamp_scroll(body)
        x = 1
        for header, _, w in columns:
            self.add(3, x, header[:w].ljust(w), curses.A_BOLD | curses.A_UNDERLINE)
            x += w + 1
        if not self.jobs:
            empty = (
                "no jobs match the filter"
                if self.filter
                else "no jobs yet — add one with: koyomi add ID --cron '...' -- CMD"
            )
            self.add(body_top + 1, 2, empty, self.color("dim"))
        for row, job in enumerate(self.jobs[self.scroll : self.scroll + body]):
            self.draw_row(body_top + row, columns, job)
        if detail:
            y = height - 2 - len(detail) - 1
            self.add(y, 0, "─" * (width - 1), self.color("dim"))
            for i, (label, value, color) in enumerate(detail, 1):
                self.add(y + i, 1, f"{label:<9}", self.color("dim"))
                self.add(y + i, 11, value, self.color(color))
        self.draw_footer(height, width)

    def draw_header(self, width: int) -> None:
        import curses

        run_count = sum(bool(j.get("running")) for j in self.all_jobs)
        fail_count = sum(
            (j.get("last_run") or {}).get("status") in FAILURE_STATES
            for j in self.all_jobs
        )
        tick = fmt_rel((self.daemon or {}).get("last_tick")) or "never"
        self.fill(0, curses.A_REVERSE)
        self.add(0, 1, "KOYOMI", curses.A_REVERSE | curses.A_BOLD)
        right = (
            f"scheduler {'running' if self.alive else 'DOWN'} · tick {tick} "
            if self.alive
            else "scheduler DOWN — koyomi service start "
        )
        self.add(0, max(9, width - len(right) - 1), right, curses.A_REVERSE)
        parts = [
            (
                f"{len(self.all_jobs)} job" + ("s" if len(self.all_jobs) != 1 else ""),
                "",
            ),
            (f"{sum(bool(j.get('enabled')) for j in self.all_jobs)} enabled", ""),
            (f"{run_count} running", "cyan" if run_count else "dim"),
            (f"{fail_count} failing", "red" if fail_count else "dim"),
            (f"sort: {self.sort}", "dim"),
        ]
        x = 1
        for text, color in parts:
            self.add(1, x, text, self.color(color))
            x += len(text) + 3
        if self.filter or self.mode == "filter":
            label = f"/{self.filter}" + ("_" if self.mode == "filter" else "")
            self.add(2, 1, label[: width - 2], self.color("yellow"))

    def layout(self, width: int) -> list[tuple[str, str, int]]:
        """Drop optional columns right-to-left, then fit JOB to the ids on screen."""
        columns = list(TUI_COLUMNS)
        natural = min(28, max(6, *(len(j["id"]) for j in self.jobs), len("JOB")))
        while len(columns) > 2:
            used = sum(max(w, natural) for _, _, w in columns) + len(columns) + 1
            if width - used >= 16:
                break
            columns.pop()
        fixed = sum(w for _, _, w in columns if w > 0) + len(columns) + 1
        for i, (header, key, w) in enumerate(columns):
            if w == 0:
                columns[i] = (header, key, natural)
                fixed += natural
        sched = max(
            (len(describe_schedule(j["schedule"])) for j in self.jobs), default=10
        )
        for i, (header, key, w) in enumerate(columns):
            if w == -1:
                columns[i] = (header, key, max(10, min(sched, width - fixed - 1)))
        return columns

    def draw_row(self, y: int, columns, job: dict) -> None:
        import curses

        selected = job["id"] == self.selected_id
        base = curses.A_REVERSE if selected else 0
        if selected:
            self.fill(y, base)
        cells = self.row_cells(job)
        x = 1
        for _, key, w in columns:
            text, color = cells[key]
            attr = base if selected else self.color(color)
            self.add(y, x, text[:w].ljust(w), attr)
            x += w + 1

    def row_cells(self, job: dict) -> dict[str, tuple[str, str]]:
        state = dashboard_status(job)
        last = job.get("last_run") or {}
        status = last.get("status") or "—"
        if job.get("next_run"):
            nxt, nxt_color = fmt_rel(job["next_run"]), ""
        elif not job.get("enabled"):
            nxt, nxt_color = "paused", "dim"
        else:
            nxt, nxt_color = "—", "dim"
        return {
            "id": (job["id"], "dim" if not job.get("enabled") else ""),
            "state": (state, STATE_COLORS.get(state, "")),
            "schedule": (describe_schedule(job["schedule"]), "dim"),
            "next": (nxt, nxt_color),
            "last": (status, RUN_COLORS.get(status, "dim")),
            "when": (
                fmt_rel(last.get("finished_at") or last.get("started_at")) or "—",
                "dim",
            ),
        }

    def detail_lines(self) -> list[tuple[str, str, str]]:
        job = self.job
        assert job is not None
        last = job.get("last_run") or {}
        rows = [("command", job["command"], "")]
        if job.get("description"):
            rows.append(("about", job["description"], "dim"))
        rows.append(("cwd", job["cwd"], "dim"))
        extras = f"catchup {job['catchup']} · timeout {job.get('timeout') or 'none'}"
        if job.get("env"):
            extras += f" · env {' '.join(job['env'])}"
        if job.get("next_run"):
            extras += f" · next {fmt_time(job['next_run'])}"
        rows.append(("options", extras, "dim"))
        if job.get("running"):
            r = job["running"]
            rows.append(
                (
                    "now",
                    f"run {r['run_id']} pid {r['pid']} started {fmt_rel(r['started_at'])}",
                    "cyan",
                )
            )
        if last:
            rows.append(
                (
                    "last run",
                    f"{last['status']} exit={last['exit_code']} ({last['trigger']}) "
                    f"{fmt_time(last['started_at'])}"
                    + (f" — {last['error']}" if last.get("error") else ""),
                    RUN_COLORS.get(last["status"], ""),
                )
            )
        return rows

    def draw_footer(self, height: int, width: int) -> None:
        import curses

        if self.mode == "confirm" and self.confirm:
            self.fill(height - 2, self.color("red") | curses.A_REVERSE)
            self.add(
                height - 2,
                1,
                f"{self.confirm[0]}  [y/n]",
                self.color("red") | curses.A_REVERSE | curses.A_BOLD,
            )
        elif self.message:
            text, color, at = self.message
            if time.monotonic() - at > 6:
                self.message = None
            else:
                self.add(height - 2, 1, text, self.color(color) | curses.A_BOLD)
        hint = (
            "type to filter · Enter keep · Esc clear"
            if self.mode == "filter"
            else "j/k move · Enter detail · e toggle · r run · l log · "
            "h history · / filter · ? help · q quit"
        )
        self.fill(height - 1, curses.A_REVERSE)
        self.add(height - 1, 1, hint[: width - 2], curses.A_REVERSE)

    # ---------------------------------------------------------------- pager

    def open_pager(self, title: str, source, follow: bool = False) -> None:
        self.pager = {
            "title": title,
            "source": source,
            "lines": source(),
            "scroll": 0,
            "follow": follow,
        }

    def page_size(self) -> int:
        return max(1, self.screen.getmaxyx()[0] - 3)

    def draw_pager(self) -> None:
        import curses

        assert self.pager is not None
        height, width = self.screen.getmaxyx()
        lines, body = self.pager["lines"], self.page_size()
        max_scroll = max(0, len(lines) - body)
        if self.pager["follow"]:
            self.pager["scroll"] = max_scroll
        self.pager["scroll"] = max(0, min(self.pager["scroll"], max_scroll))
        top = self.pager["scroll"]
        self.fill(0, curses.A_REVERSE)
        self.add(0, 1, self.pager["title"], curses.A_REVERSE | curses.A_BOLD)
        pos = f"{top + 1}-{min(len(lines), top + body)}/{len(lines)} "
        if self.pager["follow"] and top == max_scroll:
            pos = "live " + pos
        self.add(0, max(1, width - len(pos) - 1), pos, curses.A_REVERSE)
        for i, line in enumerate(lines[top : top + body], 1):
            self.add(i, 1, line.replace("\t", "    "))
        if not lines:
            self.add(2, 2, "(empty)", self.color("dim"))
        self.fill(height - 1, curses.A_REVERSE)
        self.add(
            height - 1,
            1,
            "j/k scroll · Ctrl-D/U page · g/G top/end · f follow · q close",
            curses.A_REVERSE,
        )

    def handle_pager_key(self, key: int) -> None:
        import curses

        assert self.pager is not None
        body = self.page_size()
        if key in (ord("q"), 27, curses.KEY_LEFT):
            self.pager = None
            return
        if key in (curses.KEY_DOWN, ord("j")):
            self.pager["scroll"] += 1
            self.pager["follow"] = False
        elif key in (curses.KEY_UP, ord("k")):
            self.pager["scroll"] -= 1
            self.pager["follow"] = False
        elif key in (curses.KEY_NPAGE, 4):  # Ctrl-D
            self.pager["scroll"] += body
            self.pager["follow"] = False
        elif key in (curses.KEY_PPAGE, 21):  # Ctrl-U
            self.pager["scroll"] -= body
            self.pager["follow"] = False
        elif key in (ord("g"), curses.KEY_HOME):
            self.pager["scroll"], self.pager["follow"] = 0, False
        elif key in (ord("G"), curses.KEY_END):
            self.pager["scroll"] = len(self.pager["lines"])
        elif key == ord("f"):
            self.pager["follow"] = not self.pager["follow"]
        elif key == 12:
            self.screen.clear()
        else:
            return
        self.pager["lines"] = self.pager["source"]()

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
        if self.pager:
            self.handle_pager_key(key)
            return False
        if self.mode == "filter":
            self.handle_filter_key(key)
            return False
        if self.mode == "confirm":
            assert self.confirm is not None
            action = self.confirm[1]
            self.mode, self.confirm = None, None
            if key in (ord("y"), ord("Y")):
                self.guard(action)  # type: ignore[arg-type]
            else:
                self.notify("cancelled", "dim")
            return False
        return self.handle_dashboard_key(key)

    def handle_dashboard_key(self, key: int) -> bool:
        import curses

        job = self.job
        body = max(1, self.screen.getmaxyx()[0] - 7)
        if key == ord("q"):
            return True
        if key == 27:
            if self.filter:
                self.filter = ""
                self.apply_view()
            else:
                return True
        elif key in (curses.KEY_DOWN, ord("j")):
            self.move(1)
        elif key in (curses.KEY_UP, ord("k")):
            self.move(-1)
        elif key in (curses.KEY_NPAGE, 4):
            self.move(body)
        elif key in (curses.KEY_PPAGE, 21):
            self.move(-body)
        elif key in (ord("g"), curses.KEY_HOME):
            self.move(-len(self.jobs))
        elif key in (ord("G"), curses.KEY_END):
            self.move(len(self.jobs))
        elif key in (10, 13, curses.KEY_ENTER, curses.KEY_RIGHT):
            self.expanded = not self.expanded
        elif key == ord("s"):
            self.sort = SORTS[(SORTS.index(self.sort) + 1) % len(SORTS)]
            self.apply_view()
        elif key == ord("/"):
            self.mode = "filter"
        elif key == ord("?"):
            self.open_pager(
                "KEYS",
                lambda: (
                    [f"  {k:<14}{d}" for k, d in TUI_KEYS]
                    + ["", "  State and history come from ~/.koyomi/, same as the CLI."]
                ),
            )
        elif key == ord("D"):
            self.open_pager(
                "SCHEDULER LOG", lambda: tui_tail(home() / "daemon.log", 500), True
            )
        elif key == ord("l") and job:
            self.open_pager(
                f"LOG · {job['id']}", lambda i=job["id"]: self.log_lines(i), True
            )
        elif key == ord("h") and job:
            self.open_pager(
                f"HISTORY · {job['id']}", lambda i=job["id"]: self.history_lines(i)
            )
        elif key == ord("e") and job:
            self.guard(
                lambda: self.notify(
                    f"{job['id']} "
                    + (
                        "disabled"
                        if not set_job_enabled(job["id"], not job.get("enabled"))[
                            "enabled"
                        ]
                        else "enabled"
                    ),
                    "yellow",
                )
            )
        elif key == ord("r") and job:
            self.guard(
                lambda: self.notify(
                    f"{job['id']}: started run {start_detached_run(job['id'])['run_id']}",
                    "cyan",
                )
            )
        elif key == ord("x") and job:
            self.ask(
                f"stop the active run of {job['id']}?",
                lambda: self.notify(
                    f"{job['id']}: stopping {stop_run(job['id'])}", "yellow"
                ),
            )
        elif key == ord("d") and job:
            self.ask(
                f"delete {job['id']} and its run history?",
                lambda: self.notify(f"deleted {delete_job(job['id'])['id']}", "red"),
            )
        elif key == 12:  # Ctrl-L
            self.screen.clear()
        return False

    def handle_filter_key(self, key: int) -> None:
        import curses

        if key == 27:
            self.filter, self.mode = "", None
        elif key in (10, 13, curses.KEY_ENTER):
            self.mode = None
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            self.filter = self.filter[:-1]
        elif 32 <= key < 127:
            self.filter += chr(key)
        else:
            return
        self.apply_view()

    def ask(self, question: str, action) -> None:
        self.mode, self.confirm, self.message = "confirm", (question, action), None

    def guard(self, action) -> None:
        try:
            action()
        except (KoyomiError, OSError) as e:
            self.notify(str(e), "red")
        self.reload()

    def log_lines(self, job_id: str) -> list[str]:
        try:
            job = load_job(job_id)
        except KoyomiError as e:
            return [str(e)]
        path = job_log_path(job)
        if path is None:
            return ["no runs yet — press r to start one"]
        return tui_tail(path, 500) or ["(no output yet)"]

    def history_lines(self, job_id: str) -> list[str]:
        runs = sorted(load_runs(job_id), key=lambda r: r["started_at"], reverse=True)[
            :200
        ]
        return [
            f"  {fmt_time(r['started_at'])}  {r['status']:<12} "
            f"{fmt_dur(r.get('duration_seconds')):>8}  {r['trigger']:<9} "
            f"{r.get('error') or ''}"
            for r in runs
        ] or ["no runs recorded yet"]


# ---------------------------------------------------------------- cli


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="koyomi",
        description="Tiny local job scheduler. State: ~/.koyomi/",
        epilog="Command after '--' is shell-quoted; use --cmd for pipes/redirects.",
    )
    p.add_argument("--version", action="version", version=f"koyomi {VERSION}")
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")

    def job_options(sp, adding: bool):
        g = sp.add_argument_group("schedule (pick one)")
        g.add_argument(
            "--cron", help="5-field cron in local time, e.g. '30 9 * * 1-5' or @daily"
        )
        g.add_argument("--every", help="fixed interval, e.g. 15m, 2h, 1d")
        g.add_argument("--at", help="one-time: 'YYYY-MM-DD HH:MM' or 'HH:MM'")
        g.add_argument(
            "--in", dest="in_", metavar="DURATION", help="one-time, relative: e.g. 10m"
        )
        sp.add_argument("--cmd", help="shell command (run with /bin/sh -c)")
        sp.add_argument(
            "--cwd",
            help="working directory" + (" (default: current)" if adding else ""),
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
            help="after sleep/shutdown: run a missed slot once (default) or skip it",
        )
        sp.add_argument(
            "--timeout",
            help="kill the run after DURATION"
            + ("" if adding else " ('none' to clear)"),
        )
        sp.add_argument(
            "--notify",
            action=argparse.BooleanOptionalAction,
            default=None,
            help="macOS notification when a run fails",
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

    sp = sub.add_parser("run", help="run a job now (foreground, output streamed)")
    sp.add_argument("id")
    sp.add_argument("--detach", "-d", action="store_true", help="run in background")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("stop", help="interrupt a job's active run")
    sp.add_argument("id")
    sp.set_defaults(func=cmd_stop)

    sp = sub.add_parser("history", help="past runs (all jobs or one)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("-n", "--limit", type=int, default=20)
    sp.add_argument("--failed", action="store_true", help="only non-successful runs")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("logs", help="output of a job's latest run (or scheduler log)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("--run", help="specific run id")
    sp.add_argument(
        "-n", "--lines", type=int, default=100, help="tail N lines (0 = all)"
    )
    sp.add_argument("--daemon", action="store_true", help="show the scheduler log")
    sp.set_defaults(func=cmd_logs)

    sp = sub.add_parser("status", help="scheduler health, upcoming and failing jobs")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser(
        "tui", aliases=["ui"], help="interactive job dashboard and controls"
    )
    sp.add_argument(
        "--refresh",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="screen refresh interval, 0.2–60 seconds (default: 1)",
    )
    sp.set_defaults(func=cmd_tui)

    sp = sub.add_parser("service", help="manage the launchd agent")
    sp.add_argument(
        "action", choices=["install", "uninstall", "start", "stop", "restart", "status"]
    )
    sp.set_defaults(func=cmd_service)

    sp = sub.add_parser(
        "daemon", help="run the scheduler in the foreground (launchd uses this)"
    )
    sp.set_defaults(func=lambda a: daemon_main())

    sp = sub.add_parser("_exec")  # internal: runner for a claimed run
    sp.add_argument("id")
    sp.add_argument("run_id")
    sp.set_defaults(func=lambda a: execute(a.id, a.run_id))
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
