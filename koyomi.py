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
TICK_SECONDS = 15          # max sleep between scheduler checks
GRACE_SECONDS = 300        # a run later than this counts as "missed"
KEEP_RUNS = 50             # run records kept per job
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
        raise KoyomiError(f"invalid duration {text!r} (examples: 30s, 15m, 2h, 1d, 1h30m)")
    return sum(int(n) * units[u] for n, u in parts)


def parse_when(text: str) -> dt.datetime:
    """'HH:MM' (next occurrence) or ISO-ish 'YYYY-MM-DD HH:MM[:SS][+offset]'."""
    v = text.strip()
    if re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", v):
        parts = [int(x) for x in v.split(":")]
        cur = now()
        t = cur.replace(hour=parts[0], minute=parts[1],
                        second=parts[2] if len(parts) > 2 else 0, microsecond=0)
        return t if t > cur else t + dt.timedelta(days=1)
    try:
        t = dt.datetime.fromisoformat(v)
    except ValueError:
        raise KoyomiError(f"invalid time {text!r} (use 'YYYY-MM-DD HH:MM' or 'HH:MM')") from None
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
    "@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *",
}
MONTH_NAMES = {m: i + 1 for i, m in enumerate(
    "jan feb mar apr may jun jul aug sep oct nov dec".split())}
DOW_NAMES = {d: i for i, d in enumerate("sun mon tue wed thu fri sat".split())}


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
            raise KoyomiError(f"invalid cron {expr!r}: need 5 fields (minute hour day month weekday)")
        try:
            self.minutes = sorted(_cron_field(fields[0], 0, 59, {}))
            self.hours = sorted(_cron_field(fields[1], 0, 23, {}))
            self.days = _cron_field(fields[2], 1, 31, {})
            self.months = _cron_field(fields[3], 1, 12, MONTH_NAMES)
            self.dows = {d % 7 for d in _cron_field(fields[4], 0, 7, DOW_NAMES)}
        except ValueError as e:
            raise KoyomiError(f"invalid cron {expr!r}: bad value {e}") from None
        # Vixie cron: if both day fields are restricted, a day matches either.
        self.either_day = not fields[2].startswith("*") and not fields[4].startswith("*")

    def day_matches(self, d: dt.date) -> bool:
        if d.month not in self.months:
            return False
        dom, dow = d.day in self.days, d.isoweekday() % 7 in self.dows
        return (dom or dow) if self.either_day else (dom and dow)

    def next_after(self, after: dt.datetime) -> dt.datetime:
        start = after.astimezone().replace(tzinfo=None, second=0, microsecond=0) + dt.timedelta(minutes=1)
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
        if after < anchor:
            return anchor
        k = int((after - anchor).total_seconds() // secs) + 1
        return anchor + dt.timedelta(seconds=k * secs)
    at = parse_iso(s["at"])
    return at if at > after else None


def boot_time() -> dt.datetime | None:
    global _BOOT_TIME
    if _BOOT_TIME is None:
        try:
            out = subprocess.run(["sysctl", "-n", "kern.boottime"],
                                 capture_output=True, text=True).stdout
            m = re.search(r"sec = (\d+)", out)
            _BOOT_TIME = dt.datetime.fromtimestamp(int(m.group(1))).astimezone() if m else False
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
    keys = ("run_id", "trigger", "status", "exit_code", "started_at", "finished_at", "error")
    return {k: rec.get(k) for k in keys}


def new_run_record(job: dict, trigger: str, cur: dt.datetime, scheduled_for=None) -> dict:
    run_id = cur.strftime("%Y%m%dT%H%M%S") + "-" + os.urandom(2).hex()
    return {
        "run_id": run_id, "job_id": job["id"], "trigger": trigger, "status": "running",
        "scheduled_for": iso(scheduled_for), "started_at": iso(cur), "finished_at": None,
        "duration_seconds": None, "exit_code": None, "error": None,
        "command": job["command"], "cwd": job["cwd"],
        "log": str(run_log_path(job["id"], run_id)),
    }


def claim_run(job: dict, trigger: str, cur: dt.datetime, scheduled_for=None) -> dict:
    """Create a 'running' record and mark the job as running (caller holds lock and saves)."""
    rec = new_run_record(job, trigger, cur, scheduled_for)
    write_json(run_json_path(job["id"], rec["run_id"]), rec)
    job["running"] = {"run_id": rec["run_id"], "pid": None,
                      "started_at": rec["started_at"], "trigger": trigger}
    return rec


def record_skip(job: dict, reason: str, scheduled_for: dt.datetime, cur: dt.datetime) -> None:
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
        rec.update(status="interrupted", finished_at=iso(now()),
                   error="runner disappeared (reboot, crash or kill) before finishing")
        write_json(path, rec)
    if rec:
        job["last_run"] = run_summary(rec)
    job["running"] = None
    log_event(f"{job['id']}: run {r['run_id']} marked interrupted (runner pid {pid} gone)")
    return True


def spawn_runner(job_id: str, run_id: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "_exec", job_id, run_id],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True)


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
                    record_skip(job, f"missed by {fmt_dur(late)} (catchup=skip)", due, cur)
                    write_json(path, job)
                else:
                    rec = claim_run(job, "schedule", cur, due)
                    write_json(path, job)
                    if late > GRACE_SECONDS:
                        log_event(f"{job['id']}: catch-up run for missed slot {iso(due)} "
                                  f"({fmt_dur(late)} late)")
                    log_event(f"{job['id']}: starting run {rec['run_id']}")
                    try:
                        proc = spawn_runner(job["id"], rec["run_id"])
                        job["running"]["pid"] = proc.pid
                        launched.append(proc)
                    except OSError as e:
                        rec.update(status="failed", finished_at=iso(now()),
                                   error=f"could not spawn runner: {e}")
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
    script = ['-e', 'on run argv', '-e',
              'display notification (item 2 of argv) with title (item 1 of argv)', '-e', 'end run']
    with contextlib.suppress(OSError):
        subprocess.run(["osascript", *script, title, message],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)


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
    env = {**os.environ, **(job.get("env") or {}), "KOYOMI_JOB": job_id, "KOYOMI_RUN_ID": run_id}
    timeout = parse_duration(job["timeout"]) if job.get("timeout") else None
    status, exit_code, error = "failed", None, None
    started = time.time()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as log:
        try:
            proc = subprocess.Popen(["/bin/sh", "-c", job["command"]], cwd=job["cwd"], env=env,
                                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True)
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
                status, exit_code, error = "timeout", proc.returncode, f"timed out after {job['timeout']}"
            except KeyboardInterrupt:
                kill_group(proc)
                status, exit_code, error = "interrupted", proc.returncode, "interrupted by user"
    if echo:
        wait_for_echo_flush(log_path)
    rec.update(status=status, exit_code=exit_code, error=error, finished_at=iso(now()),
               duration_seconds=round(time.time() - started, 3))
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


def wait_for(proc: subprocess.Popen, log_path: Path, timeout: int | None, echo: bool) -> int:
    if not echo:
        return proc.wait(timeout=timeout)
    deadline = time.time() + timeout if timeout else None
    while True:
        rc = proc.poll()
        _echo(log_path)
        if rc is not None:
            return rc
        if deadline and time.time() > deadline:
            raise subprocess.TimeoutExpired(proc.args, timeout)
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
            log_event(f"clock jumped {fmt_dur(gap)} since last tick (sleep/suspend); checking missed jobs")
        last = cur
        earliest = None
        try:
            _, earliest = tick(cur)
        except Exception:
            log_event("tick error:\n" + traceback.format_exc())
        with contextlib.suppress(OSError):
            write_json(home() / "daemon.json", {"pid": os.getpid(), "version": VERSION,
                                                "started_at": iso(started), "last_tick": iso(cur)})
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
    old_pid = info["pid"] if alive else None
    if action == "install":
        home().mkdir(parents=True, exist_ok=True)
        plist = {
            "Label": LABEL,
            "ProgramArguments": [sys.executable, str(Path(__file__).resolve()), "daemon"],
            "RunAtLoad": True,
            "KeepAlive": True,
            "EnvironmentVariables": {"PATH": ":".join(dict.fromkeys(
                                         os.environ.get("PATH", "/usr/bin:/bin").split(":"))),
                                     "KOYOMI_HOME": str(home())},
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
        print("scheduler stopped and launchd agent removed (jobs in ~/.koyomi are kept)")
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
        print("scheduler stopped (starts again at next login, or: koyomi service start)")
    elif action == "restart":
        r = launchctl("kickstart", "-k", target)
        if r.returncode != 0:
            raise KoyomiError(f"launchctl kickstart failed: {r.stderr.strip() or 'not loaded'}")
        wait_for_daemon(old_pid)
        print("scheduler restarted")
    elif action == "status":
        print_daemon_status()
    return 0


def wait_for_daemon(previous_pid=None, seconds: float = 5) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        alive, info = daemon_state()
        if alive and info["pid"] != previous_pid:
            return
        time.sleep(0.2)


def print_daemon_status() -> None:
    alive, info = daemon_state()
    loaded = service_loaded()
    if alive:
        print(f"scheduler: running (pid {info['pid']}, last tick {fmt_rel(info['last_tick'])})")
    else:
        print("scheduler: NOT running")
    print(f"launchd:   {'loaded' if loaded else 'not loaded'} ({LABEL})"
          + ("" if plist_path().exists() else " - agent not installed, run: koyomi service install"))
    print(f"home:      {home()}")


# ---------------------------------------------------------------- commands

def schedule_from_args(args, required: bool) -> dict | None:
    given = [(k, v) for k in ("cron", "every", "at", "in_") if (v := getattr(args, k, None))]
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
    return {"at": iso(now().replace(microsecond=0) + dt.timedelta(seconds=parse_duration(val)))}


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
        raise KoyomiError("job id must be letters, digits, '.', '_' or '-' (max 64 chars)")
    command = resolve_command(args)
    if not command:
        raise KoyomiError("a command is required: --cmd 'shell command' or -- program args")
    cwd = str(Path(args.cwd or os.getcwd()).expanduser().resolve())
    if not Path(cwd).is_dir():
        raise KoyomiError(f"working directory does not exist: {cwd}")
    if args.timeout:
        parse_duration(args.timeout)
    ts = iso(now())
    job = {
        "id": args.id, "description": args.description or "", "command": command, "cwd": cwd,
        "env": parse_env(args.env), "schedule": schedule_from_args(args, required=True),
        "enabled": not args.disabled, "catchup": args.catchup or "once",
        "timeout": args.timeout, "notify": bool(args.notify),
        "created_at": ts, "updated_at": ts, "next_run": None, "last_run": None, "running": None,
    }
    refresh_next_run(job)
    with store_lock():
        if job_path(args.id).exists():
            raise KoyomiError(f"job already exists: {args.id} (use: koyomi update)")
        write_json(job_path(args.id), job)
    print(f"added {args.id}: {describe_schedule(job['schedule'])}; "
          f"next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}".rstrip())
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
            job["timeout"] = None if args.timeout.lower() in ("none", "0") else args.timeout
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
    print(f"updated {args.id}; next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}".rstrip())
    return 0


def cmd_enable(args, enabled: bool) -> int:
    with store_lock():
        job = load_job(args.id)
        job["enabled"] = enabled
        refresh_next_run(job)  # re-enabling never back-fills the disabled period
        job["updated_at"] = iso(now())
        write_json(job_path(args.id), job)
    if enabled:
        print(f"enabled {args.id}; next run {fmt_time(job['next_run'])} {fmt_rel(job['next_run'])}")
        warn_if_daemon_down()
    else:
        print(f"disabled {args.id}")
    return 0


def cmd_delete(args) -> int:
    import shutil
    with store_lock():
        job = load_job(args.id)
        job_path(args.id).unlink()
        if not args.keep_logs:
            shutil.rmtree(runs_dir(args.id), ignore_errors=True)
    if job.get("running"):
        print(f"note: run {job['running']['run_id']} (pid {job['running']['pid']}) is still in progress",
              file=sys.stderr)
    print(f"deleted {args.id}" + (" (run history kept)" if args.keep_logs else ""))
    return 0


def cmd_run(args) -> int:
    with store_lock():
        job = load_job(args.id)
        reconcile_running(job)
        if job.get("running"):
            r = job["running"]
            raise KoyomiError(f"{args.id} is already running (run {r['run_id']}, pid {r['pid']})")
        rec = claim_run(job, "manual", now())
        job["running"]["pid"] = os.getpid()
        write_json(job_path(args.id), job)
        if args.detach:
            proc = spawn_runner(args.id, rec["run_id"])
            job["running"]["pid"] = proc.pid
            write_json(job_path(args.id), job)
    log_event(f"{args.id}: manual run {rec['run_id']}" + (" (detached)" if args.detach else ""))
    if args.detach:
        print(f"started {args.id} run {rec['run_id']}; log: {rec['log']}")
        return 0
    code = execute(args.id, rec["run_id"], echo=True)
    final = read_json(run_json_path(args.id, rec["run_id"])) if job_path(args.id).exists() else rec
    print(f"--- koyomi: {args.id} {final['status']} "
          f"(exit {final['exit_code']}, {fmt_dur(final['duration_seconds'])}) run {rec['run_id']}",
          file=sys.stderr)
    return code


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
        rows.append([j["id"], job_status_word(j), describe_schedule(j["schedule"]),
                     fmt_time(j.get("next_run")),
                     f"{last.get('status', '-')} {fmt_rel(last.get('finished_at') or last.get('started_at'))}".strip()])
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
    print(f"catchup:     {job['catchup']}   timeout: {job.get('timeout') or 'none'}   "
          f"notify: {'on failure' if job.get('notify') else 'off'}")
    print(f"next run:    {fmt_time(job.get('next_run'))} {fmt_rel(job.get('next_run'))}")
    if job.get("enabled") and job.get("next_run") and "at" not in job["schedule"]:
        t, upcoming = parse_iso(job["next_run"]), []
        for _ in range(3):
            t = compute_next(job, t)
            upcoming.append(fmt_time(t))
        print(f"then:        {', '.join(upcoming)}")
    if job.get("running"):
        r = job["running"]
        print(f"running:     run {r['run_id']} pid {r['pid']} since {fmt_time(r['started_at'])}")
    if last:
        print(f"last run:    {last['status']} exit={last['exit_code']} at {fmt_time(last['started_at'])} "
              f"({last['trigger']}) run {last['run_id']}")
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
    runs = runs[-args.limit:]
    if args.json:
        print(json.dumps(runs, indent=2))
        return 0
    if not runs:
        print("no runs")
        return 0
    rows = [[r["run_id"], r["job_id"], r["trigger"], r["status"],
             "-" if r["exit_code"] is None else r["exit_code"], fmt_time(r["started_at"]),
             fmt_dur(r["duration_seconds"]), r.get("error") or ""] for r in runs]
    print_table(rows, ["RUN", "JOB", "TRIGGER", "STATUS", "EXIT", "STARTED", "DURATION", "ERROR"])
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
    rec = read_json(run_json_path(args.id, run_id)) if run_json_path(args.id, run_id).exists() else {}
    print(f"==> run {run_id}: {rec.get('status', '?')} exit={rec.get('exit_code')} "
          f"started {fmt_time(rec.get('started_at'))}", file=sys.stderr)
    print(tail_lines(run_log_path(args.id, run_id), args.lines))
    return 0


def cmd_status(args) -> int:
    print_daemon_status()
    jobs = load_jobs()
    enabled = [j for j in jobs if j.get("enabled")]
    running = [j for j in jobs if j.get("running")]
    print(f"jobs:      {len(jobs)} total, {len(enabled)} enabled, {len(running)} running")
    upcoming = sorted((j for j in enabled if j.get("next_run")), key=lambda j: j["next_run"])
    if upcoming:
        j = upcoming[0]
        print(f"next:      {j['id']} at {fmt_time(j['next_run'])} ({fmt_rel(j['next_run'])})")
    failing = [j for j in jobs if (j.get("last_run") or {}).get("status") in ("failed", "timeout", "interrupted")]
    if failing:
        print("failing:")
        for j in failing:
            lr = j["last_run"]
            print(f"  {j['id']}: {lr['status']} ({lr.get('error')}) at {fmt_time(lr['started_at'])}"
                  f" -> koyomi logs {j['id']}")
    return 0


def warn_if_daemon_down() -> None:
    alive, _ = daemon_state()
    if not alive:
        print("warning: scheduler is not running; start it with: koyomi service start", file=sys.stderr)


# ---------------------------------------------------------------- cli

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="koyomi", description="Tiny local job scheduler. State: ~/.koyomi/",
                                epilog="Command after '--' is shell-quoted; use --cmd for pipes/redirects.")
    p.add_argument("--version", action="version", version=f"koyomi {VERSION}")
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")

    def job_options(sp, adding: bool):
        g = sp.add_argument_group("schedule (pick one)")
        g.add_argument("--cron", help="5-field cron in local time, e.g. '30 9 * * 1-5' or @daily")
        g.add_argument("--every", help="fixed interval, e.g. 15m, 2h, 1d")
        g.add_argument("--at", help="one-time: 'YYYY-MM-DD HH:MM' or 'HH:MM'")
        g.add_argument("--in", dest="in_", metavar="DURATION", help="one-time, relative: e.g. 10m")
        sp.add_argument("--cmd", help="shell command (run with /bin/sh -c)")
        sp.add_argument("--cwd", help="working directory" + (" (default: current)" if adding else ""))
        sp.add_argument("--env", action="append", metavar="KEY=VALUE", help="extra env var (repeatable)")
        sp.add_argument("--description", help="free-text note")
        sp.add_argument("--catchup", choices=["once", "skip"],
                        help="after sleep/shutdown: run a missed slot once (default) or skip it")
        sp.add_argument("--timeout", help="kill the run after DURATION" + ("" if adding else " ('none' to clear)"))
        sp.add_argument("--notify", action=argparse.BooleanOptionalAction, default=None,
                        help="macOS notification when a run fails")

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

    sp = sub.add_parser("delete", aliases=["rm"], help="delete a job and its run history")
    sp.add_argument("id")
    sp.add_argument("--keep-logs", action="store_true")
    sp.set_defaults(func=cmd_delete)

    sp = sub.add_parser("run", help="run a job now (foreground, output streamed)")
    sp.add_argument("id")
    sp.add_argument("--detach", "-d", action="store_true", help="run in background")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("history", help="past runs (all jobs or one)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("-n", "--limit", type=int, default=20)
    sp.add_argument("--failed", action="store_true", help="only non-successful runs")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("logs", help="output of a job's latest run (or scheduler log)")
    sp.add_argument("id", nargs="?")
    sp.add_argument("--run", help="specific run id")
    sp.add_argument("-n", "--lines", type=int, default=100, help="tail N lines (0 = all)")
    sp.add_argument("--daemon", action="store_true", help="show the scheduler log")
    sp.set_defaults(func=cmd_logs)

    sp = sub.add_parser("status", help="scheduler health, upcoming and failing jobs")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("service", help="manage the launchd agent")
    sp.add_argument("action", choices=["install", "uninstall", "start", "stop", "restart", "status"])
    sp.set_defaults(func=cmd_service)

    sp = sub.add_parser("daemon", help="run the scheduler in the foreground (launchd uses this)")
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
        argv, command_argv = argv[:i], argv[i + 1:]
    args = build_parser().parse_args(argv)
    args.argv_command = command_argv
    if command_argv and args.command not in ("add", "update"):
        print("koyomi: error: '--' command is only valid for add/update", file=sys.stderr)
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
