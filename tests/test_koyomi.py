import base64
import contextlib
import datetime as dt
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import koyomi as k  # noqa: E402

KOYOMI_PY = str(Path(k.__file__).resolve())
TORONTO = ZoneInfo("America/Toronto")
EMAIL = {
    "smtp_host": "smtp.invalid",
    "smtp_port": 587,
    "smtp_user": "u",
    "smtp_password": "p",
    "from": "koyomi@example.com",
    "to": "me@example.com",
}


def toronto(*args):
    return dt.datetime(*args, tzinfo=TORONTO)


def unstamp(log: str) -> str:
    """The log without its line stamps; fails if a line is unstamped."""
    lines = log.split("\n")
    for line in lines[:-1] if log.endswith("\n") else lines:
        assert k.LOG_STAMP.match(line), line
    return "\n".join(k.LOG_STAMP.sub("", line, count=1) for line in lines)


class CronTests(unittest.TestCase):
    def test_basic(self):
        c = k.Cron("30 9 * * *")
        self.assertEqual(
            c.next_after(toronto(2026, 9, 13, 9, 29), TORONTO),
            toronto(2026, 9, 13, 9, 30),
        )
        self.assertEqual(
            c.next_after(toronto(2026, 9, 13, 9, 30), TORONTO),
            toronto(2026, 9, 14, 9, 30),
        )

    def test_steps_ranges_names(self):
        c = k.Cron("*/15 9-17 * * mon-fri")
        # 2026-09-13 is a Sunday
        self.assertEqual(
            c.next_after(toronto(2026, 9, 13, 12, 0), TORONTO),
            toronto(2026, 9, 14, 9, 0),
        )
        self.assertEqual(
            c.next_after(toronto(2026, 9, 14, 9, 7), TORONTO),
            toronto(2026, 9, 14, 9, 15),
        )
        self.assertEqual(
            c.next_after(toronto(2026, 9, 14, 17, 45), TORONTO),
            toronto(2026, 9, 15, 9, 0),
        )

    def test_dom_dow_or(self):
        c = k.Cron("0 0 1 * sun")  # 1st of month OR Sunday
        self.assertEqual(
            c.next_after(toronto(2026, 9, 13, 1, 0), TORONTO),
            toronto(2026, 9, 20, 0, 0),
        )
        self.assertEqual(
            c.next_after(toronto(2026, 9, 27, 1, 0), TORONTO),
            toronto(2026, 10, 1, 0, 0),
        )

    def test_job_zone_not_host_zone(self):
        # a server on UTC runs "6:00 Toronto" at 10:00 UTC in summer, 11:00 in winter
        c = k.Cron("0 6 * * *")
        summer = c.next_after(dt.datetime(2026, 7, 1, tzinfo=dt.timezone.utc), TORONTO)
        winter = c.next_after(dt.datetime(2026, 12, 1, tzinfo=dt.timezone.utc), TORONTO)
        self.assertEqual(summer.astimezone(dt.timezone.utc).hour, 10)
        self.assertEqual(winter.astimezone(dt.timezone.utc).hour, 11)

    def test_dst_gap_runs_after_the_jump(self):
        # 2026-03-08 02:00 Toronto jumps to 03:00; a 02:30 slot runs at 03:30 EDT
        nxt = k.Cron("30 2 * * *").next_after(toronto(2026, 3, 8, 0, 0), TORONTO)
        self.assertEqual(nxt.utcoffset(), dt.timedelta(hours=-4))
        self.assertEqual((nxt.hour, nxt.minute), (3, 30))

    def test_aliases_and_errors(self):
        self.assertEqual(
            k.Cron("@daily").next_after(toronto(2026, 1, 1, 5), TORONTO),
            toronto(2026, 1, 2),
        )
        self.assertEqual(
            k.Cron("0 0 29 2 *").next_after(toronto(2026, 3, 1), TORONTO),
            toronto(2028, 2, 29),
        )
        for bad in ("* * *", "60 * * * *", "* * * * 8", "a b c d e"):
            with self.assertRaises(k.KoyomiError):
                k.Cron(bad)
        with self.assertRaises(k.KoyomiError):
            k.Cron("0 0 31 2 *").next_after(toronto(2026, 1, 1), TORONTO)

    def test_duration(self):
        self.assertEqual(k.parse_duration("1h30m"), 5400)
        self.assertEqual(k.parse_duration("45s"), 45)
        with self.assertRaises(k.KoyomiError):
            k.parse_duration("5 minutes")


class HubCase(unittest.TestCase):
    """A hub in a temp dir; its own host 'box' is always on."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.hub_home = self.root / "hub"
        self.use(self.hub_home)
        k.write_json(k.config_path(), {"host": "box", "hub": "local", "email": EMAIL})
        k.op_register_host("box", True, sys.platform)
        self.out = self.root / "out.txt"

    def tearDown(self):
        os.environ.pop("KOYOMI_HOME")
        self.tmp.cleanup()

    def use(self, path: Path) -> None:
        os.environ["KOYOMI_HOME"] = str(path)

    def cli(self, *args) -> int:
        with open(os.devnull, "w") as devnull:
            stdout, stderr = sys.stdout, sys.stderr
            sys.stdout = sys.stderr = devnull
            try:
                return k.main(list(args))
            finally:
                sys.stdout, sys.stderr = stdout, stderr

    def add(self, *args):
        self.assertEqual(self.cli("add", *args), 0)

    def set_next_run(self, job_id, t):
        with k.store_lock():
            job = k.load_job(job_id)
            job["next_run"] = k.iso(t)
            k.save_job(job)

    def tick(self, wait=True):
        cfg = k.load_config()
        info = {"pid": os.getpid(), "version": k.VERSION, "started_at": None}
        procs = k.host_tick(cfg, info, k.now())
        if wait:
            for p in procs:
                p.wait()
        return procs

    def alerts(self):
        return [k.read_json(p) for p in sorted(k.alerts_dir().glob("*.json"))]


class SchedulerTests(HubCase):
    def test_missed_slots_collapse_to_one_late_run(self):
        self.add("j", "--cron", "0 * * * *", "--cmd", f"echo ran >> {self.out}")
        cur = k.now()
        self.set_next_run("j", cur - dt.timedelta(hours=10))  # host down for 10 slots
        self.assertEqual(len(self.tick()), 1)
        self.assertEqual(self.out.read_text(), "ran\n")
        job = k.load_job("j")
        self.assertGreater(k.parse_iso(job["next_run"]), cur)
        self.assertEqual(job["last_run"]["status"], "success")
        self.assertIsNone(job["running"])
        self.assertEqual(
            [a["subject"] for a in self.alerts()], ["Koyomi: j ran late on box"]
        )
        # a second tick must not run again
        self.assertEqual(len(self.tick()), 0)
        self.assertEqual(self.out.read_text(), "ran\n")

    def test_catchup_skip(self):
        self.add(
            "j",
            "--every",
            "1h",
            "--catchup",
            "skip",
            "--cmd",
            f"echo ran >> {self.out}",
        )
        self.set_next_run("j", k.now() - dt.timedelta(hours=3))
        self.assertEqual(len(self.tick()), 0)
        self.assertFalse(self.out.exists())
        self.assertEqual(k.load_job("j")["last_run"]["status"], "skipped")
        self.assertEqual(len(self.alerts()), 1)  # box is always on
        # but a slot that is only slightly late still runs
        self.set_next_run("j", k.now() - dt.timedelta(seconds=20))
        self.assertEqual(len(self.tick()), 1)

    def test_no_overlap_while_running(self):
        self.add("j", "--every", "1m", "--cmd", "sleep 3")
        self.set_next_run("j", k.now())
        procs = self.tick(wait=False)
        self.assertEqual(len(procs), 1)
        self.set_next_run("j", k.now())
        self.assertEqual(self.tick(wait=False), [])
        statuses = sorted(r["status"] for r in k.load_runs("j"))
        self.assertEqual(statuses, ["running", "skipped"])
        self.assertIn("skipped a slot", self.alerts()[0]["subject"])
        procs[0].wait()
        self.assertEqual(k.load_job("j")["last_run"]["status"], "success")

    def test_one_time_job_runs_once(self):
        self.add("once", "--in", "1m", "--cmd", f"echo ran >> {self.out}")
        with k.store_lock():  # its moment has come
            job = k.load_job("once")
            job["schedule"]["at"] = job["next_run"] = k.iso(
                k.now() - dt.timedelta(seconds=5)
            )
            k.save_job(job)
        self.assertEqual(len(self.tick()), 1)
        self.assertIsNone(k.load_job("once")["next_run"])
        self.assertEqual(len(self.tick()), 0)
        self.assertEqual(self.out.read_text(), "ran\n")

    def test_failure_and_timeout_recorded_and_alerted(self):
        self.add("bad", "--every", "1h", "--cmd", "echo boom; exit 3")
        self.add("slow", "--every", "1h", "--timeout", "1s", "--cmd", "sleep 30")
        self.set_next_run("bad", k.now())
        self.set_next_run("slow", k.now())
        self.tick()
        bad = k.load_runs("bad")[-1]
        self.assertEqual((bad["status"], bad["exit_code"]), ("failed", 3))
        self.assertIn("boom", bad["output_tail"])
        self.assertEqual(k.load_runs("slow")[-1]["status"], "timeout")
        alerts = {a["subject"]: a for a in self.alerts()}
        self.assertIn("boom", alerts["Koyomi: bad failed on box"]["body"])
        self.assertIn("Koyomi: slow timeout on box", alerts)

    def test_one_alert_per_failure_streak(self):
        self.add("bad", "--every", "1h", "--cmd", "exit 1")
        for _ in range(2):
            self.set_next_run("bad", k.now())
            self.tick()
        self.assertEqual(len(self.alerts()), 1)
        with k.store_lock():
            job = k.load_job("bad")
            job["command"] = "true"
            k.save_job(job)
        self.set_next_run("bad", k.now())
        self.tick()
        self.assertFalse(k.load_job("bad")["alerted"])
        with k.store_lock():
            job = k.load_job("bad")
            job["command"] = "exit 1"
            k.save_job(job)
        self.set_next_run("bad", k.now())
        self.tick()
        self.assertEqual(len(self.alerts()), 2)

    def test_dead_runner_is_reconciled(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        with k.store_lock():
            job = k.load_job("j")
            rec = k.claim_run(job, "schedule", k.now() - dt.timedelta(minutes=5))
            dead = subprocess.Popen(["true"])
            dead.wait()
            job["running"]["pid"] = dead.pid
            k.save_job(job)
        k.spool_path(rec["run_id"]).parent.mkdir(parents=True)
        k.spool_path(rec["run_id"]).write_text("last words\n")
        self.tick()
        job = k.load_job("j")
        self.assertIsNone(job["running"])
        self.assertEqual(k.load_run("j", rec["run_id"])["status"], "interrupted")
        # spooled output the runner never uploaded is salvaged
        self.assertEqual(k.run_log_path("j", rec["run_id"]).read_text(), "last words\n")
        self.assertIn("interrupted", self.alerts()[0]["subject"])

    def test_disabled_and_reenable_do_not_backfill(self):
        self.add("j", "--every", "1h", "--cmd", f"echo ran >> {self.out}")
        self.cli("disable", "j")
        self.assertEqual(len(self.tick()), 0)
        self.cli("enable", "j")
        self.assertGreater(k.parse_iso(k.load_job("j")["next_run"]), k.now())

    def test_json_is_plain(self):
        self.add(
            "j",
            "--cron",
            "@daily",
            "--tz",
            "America/Toronto",
            "--",
            "echo",
            "hello world",
        )
        data = json.loads(k.job_path("j").read_text())
        self.assertEqual(data["command"], "echo 'hello world'")
        self.assertEqual(data["schedule"], {"cron": "@daily"})
        self.assertEqual((data["host"], data["timezone"]), ("box", "America/Toronto"))

    def test_job_validation(self):
        self.assertEqual(
            self.cli(
                "add",
                "x",
                "--every",
                "1h",
                "--cmd",
                "true",
                "--host",
                "nope",
                "--cwd",
                "/tmp",
            ),
            1,
        )
        k.op_register_host("other", True, "linux")
        # another host's directory can't be checked here, but must be absolute
        self.assertEqual(
            self.cli("add", "x", "--every", "1h", "--cmd", "true", "--host", "other"), 1
        )
        self.assertEqual(
            self.cli(
                "add",
                "x",
                "--every",
                "1h",
                "--cmd",
                "true",
                "--host",
                "other",
                "--cwd",
                "rel",
            ),
            1,
        )
        self.add(
            "x",
            "--every",
            "1h",
            "--cmd",
            "true",
            "--host",
            "other",
            "--cwd",
            "/srv/nowhere",
        )
        self.assertEqual(
            self.cli("add", "y", "--every", "1h", "--cmd", "true", "--tz", "Mars/Base"),
            1,
        )


class ControlTests(HubCase):
    def test_manual_run_streams_output(self):
        self.add("j", "--every", "1h", "--cmd", "echo hello; echo bye")
        self.tick()  # heartbeat: the host is live
        rec = k.op_queue_run("j")
        self.assertEqual(k.load_job("j")["running"]["status"], "queued")
        with self.assertRaises(k.KoyomiError):
            k.op_queue_run("j")  # already queued
        self.assertEqual(len(self.tick()), 1)
        run = k.load_run("j", rec["run_id"])
        self.assertEqual((run["status"], run["trigger"]), ("success", "manual"))
        out = k.op_log("j", None, lines=0)
        self.assertEqual(unstamp(base64.b64decode(out["data"]).decode()), "hello\nbye")
        self.assertFalse(k.spool_path(rec["run_id"]).exists())

    def test_manual_run_needs_a_live_host(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        with self.assertRaisesRegex(k.KoyomiError, "last seen never"):
            k.op_queue_run("j")

    def test_stop_request(self):
        self.add("j", "--every", "1h", "--cmd", "echo started; sleep 30")
        self.tick()
        k.op_queue_run("j")
        procs = self.tick(wait=False)
        for _ in range(50):
            if k.load_job("j")["running"]["pid"]:
                break
            time.sleep(0.1)
        k.op_request_stop("j")
        procs[0].wait(timeout=20)
        job = k.load_job("j")
        self.assertIsNone(job["running"])
        self.assertEqual(job["last_run"]["status"], "stopped")
        self.assertEqual(self.alerts(), [])  # a requested stop is not an alert
        with self.assertRaises(k.KoyomiError):
            k.op_request_stop("j")

    def test_stop_a_queued_run(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        self.tick()
        k.op_queue_run("j")
        k.op_request_stop("j")
        self.assertEqual(k.load_job("j")["last_run"]["status"], "stopped")
        self.assertEqual(self.tick(), [])

    def test_output_upload_is_idempotent(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        with k.store_lock():
            job = k.load_job("j")
            rec = k.claim_run(job, "manual", k.now())
            k.save_job(job)

        def send(offset, data):
            return k.op_run_progress(
                "j", rec["run_id"], offset, base64.b64encode(data).decode()
            )

        self.assertEqual(send(0, b"abc")["size"], 3)
        self.assertEqual(send(0, b"abc")["size"], 3)  # retry after a lost reply
        self.assertEqual(send(2, b"cde")["size"], 5)
        self.assertEqual(k.run_log_path("j", rec["run_id"]).read_bytes(), b"abcde")
        with self.assertRaises(k.KoyomiError):
            send(9, b"x")

    def test_delete_job(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        self.set_next_run("j", k.now())
        self.tick()
        self.assertEqual(self.cli("delete", "j"), 0)
        self.assertFalse(k.job_path("j").exists())
        self.assertFalse(k.runs_dir("j").exists())


class RemoteHostTests(HubCase):
    """A second machine ('mac', not always on) reaching the hub over the RPC command."""

    def setUp(self):
        super().setUp()
        self.mac_home = self.root / "mac"
        self.mac_cfg = {
            "host": "mac",
            "hub": {
                "command": [
                    "env",
                    f"KOYOMI_HOME={self.hub_home}",
                    sys.executable,
                    KOYOMI_PY,
                    "_rpc",
                ]
            },
            "email": EMAIL,
        }
        k.call_hub(
            self.mac_cfg,
            "register_host",
            {"name": "mac", "always_on": False, "platform": "darwin"},
        )
        k.write_json(self.mac_home / "config.json", self.mac_cfg)

    def test_job_runs_on_its_host(self):
        work = self.root / "work"
        work.mkdir()
        self.use(self.mac_home)
        self.add(
            "m", "--every", "1h", "--cwd", str(work), "--cmd", "pwd; echo $KOYOMI_HOST"
        )
        self.add(
            "e", "--every", "1h", "--host", "box", "--cwd", "/tmp", "--cmd", "true"
        )
        self.use(self.hub_home)
        self.set_next_run("m", k.now() - dt.timedelta(hours=8))  # the laptop slept
        self.set_next_run("e", k.now())
        self.assertEqual(len(self.tick()), 1)  # the hub host runs its own job...
        self.assertIsNone(k.load_job("m")["running"])  # ...not mac's
        self.use(self.mac_home)
        self.assertEqual(len(self.tick()), 1)  # ...mac does, through the hub
        self.assertFalse(k.jobs_dir().exists())  # no job state on the host
        self.assertEqual(list((self.mac_home / "spool").iterdir()), [])
        self.use(self.hub_home)
        run = k.load_runs("m")[-1]
        self.assertEqual(run["status"], "success")
        self.assertEqual(run["host"], "mac")
        log = k.run_log_path("m", run["run_id"]).read_text()
        self.assertEqual(unstamp(log), f"{work.resolve()}\nmac\n")
        self.assertEqual(self.alerts(), [])  # a laptop catching up is not an alert

    def test_disabled_alerts_distinguish_hub_and_local_host(self):
        self.use(self.hub_home)
        k.write_json(k.config_path(), {"host": "box", "hub": "local"})
        self.use(self.mac_home)

        for local_email in (True, False):
            with self.subTest(local_email=local_email):
                if not local_email:
                    self.mac_cfg.pop("email")
                    k.write_json(k.config_path(), self.mac_cfg)
                stdout, stderr = io.StringIO(), io.StringIO()
                with (
                    contextlib.redirect_stdout(stdout),
                    contextlib.redirect_stderr(stderr),
                ):
                    self.assertEqual(k.main(["list", "--json"]), 0)
                self.assertEqual(json.loads(stdout.getvalue()), [])
                warnings = stderr.getvalue()
                self.assertIn("email alerts disabled on the hub", warnings)
                self.assertEqual(
                    "hub outage emails disabled on this host" in warnings,
                    not local_email,
                )

    def test_errors_and_version_cross_the_wire(self):
        with self.assertRaisesRegex(k.KoyomiError, "no such job: nope"):
            k.call_hub(self.mac_cfg, "job", {"job_id": "nope"})
        request = json.dumps({"version": "0.0.1", "op": "jobs", "args": {}})
        p = subprocess.run(
            self.mac_cfg["hub"]["command"],
            input=request,
            capture_output=True,
            text=True,
        )
        self.assertIn("version mismatch", json.loads(p.stdout)["error"])


class AlertTests(HubCase):
    def test_hub_without_smtp_records_failures_without_queueing_alerts(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(k.main(["init", "box", "--hub-local", "--always-on"]), 0)
        self.assertIn("email alerts disabled", stdout.getvalue())
        self.assertNotIn("email", k.load_config())

        self.add("failed-job", "--every", "1h", "--cmd", "exit 1")
        self.set_next_run("failed-job", k.now())
        self.tick()
        self.assertEqual(k.load_job("failed-job")["last_run"]["status"], "failed")
        self.assertEqual(self.alerts(), [])

        stdout = io.StringIO()
        with (
            contextlib.redirect_stdout(stdout),
            mock.patch.object(k, "service_loaded", return_value=False),
        ):
            self.assertEqual(k.main(["status"]), 0)
        self.assertIn("email alerts disabled on the hub", stdout.getvalue())
        self.assertIn("failed-job on box: failed", stdout.getvalue())

    def test_pending_alerts_are_preserved_when_smtp_is_disabled(self):
        k.raise_alert("one", "body")
        cfg = {"host": "box", "hub": "local"}
        before = self.alerts()
        watch, cur = k.HubWatch(), k.now()
        with mock.patch.object(k, "send_email") as send, mock.patch.object(k, "say"):
            k.send_pending_alerts(cfg, cur)
            watch.unreachable(cfg, k.HubUnreachable("no route"), cur)
            watch.unreachable(
                cfg, k.HubUnreachable("no route"), cur + dt.timedelta(minutes=4)
            )
            send.assert_not_called()
        self.assertEqual(self.alerts(), before)
        self.assertEqual(watch.error, "no route")

    def test_init_rejects_partial_smtp_settings(self):
        config_before = k.config_path().read_bytes()
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(
                k.main(["init", "box", "--hub-local", "--email-to", "me@example.com"]),
                1,
            )
        self.assertIn("give --smtp-env, --email-from and --email-to together", stderr.getvalue())
        self.assertEqual(k.config_path().read_bytes(), config_before)

    def test_pending_alerts_are_sent_and_retried(self):
        k.raise_alert("one", "body")
        cur = k.now()
        with mock.patch.object(k, "send_email", side_effect=k.KoyomiError("smtp down")):
            k.send_pending_alerts(k.load_config(), cur)
        alert = self.alerts()[0]
        self.assertEqual((alert["sent_at"], alert["last_error"]), (None, "smtp down"))
        with mock.patch.object(k, "send_email") as send:
            k.send_pending_alerts(k.load_config(), cur + dt.timedelta(seconds=30))
            send.assert_not_called()  # still backing off
            k.send_pending_alerts(k.load_config(), cur + dt.timedelta(seconds=61))
            send.assert_called_once_with(k.load_config(), "one", "body")
        self.assertIsNotNone(self.alerts()[0]["sent_at"])

    def test_always_on_host_down(self):
        k.op_register_host("vps", True, "linux")
        k.op_register_host("laptop", False, "darwin")
        cfg = k.load_config()
        later = k.now() + dt.timedelta(minutes=10)
        k.check_hosts_down(cfg, later)
        k.check_hosts_down(cfg, later)
        self.assertEqual(
            [a["subject"] for a in self.alerts()], ["Koyomi: host vps is down"]
        )
        k.op_tick(
            "vps", {"pid": 1, "version": k.VERSION}
        )  # back: may alert again later
        self.assertFalse(k.load_host("vps")["down_alerted"])

    def test_hub_unreachable_from_a_host(self):
        watch, cfg, cur = k.HubWatch(), {"host": "mac", "email": EMAIL}, k.now()
        with mock.patch.object(k, "send_email") as send, mock.patch.object(k, "say"):
            watch.unreachable(cfg, k.HubUnreachable("no route"), cur)
            watch.unreachable(
                cfg, k.HubUnreachable("no route"), cur + dt.timedelta(minutes=1)
            )
            send.assert_not_called()
            watch.unreachable(
                cfg, k.HubUnreachable("no route"), cur + dt.timedelta(minutes=4)
            )
            watch.unreachable(
                cfg, k.HubUnreachable("no route"), cur + dt.timedelta(minutes=5)
            )
            send.assert_called_once()
            self.assertIn("no route", send.call_args.args[2])
            watch.reachable()
            self.assertIsNone(watch.since)


class ServiceAndUiTests(HubCase):
    def test_systemd_unit(self):
        unit = k.systemd_unit()
        self.assertIn("KillMode=process", unit)  # restarts keep runs in flight
        self.assertIn(f'Environment="KOYOMI_HOME={self.hub_home}"', unit)
        self.assertIn('Environment="HOME=', unit)
        self.assertIn(" daemon\n", unit)

    def test_snapshot_history_is_per_job_and_preserves_run_host(self):
        self.add("busy", "--every", "1m", "--cmd", "true")
        self.add("quiet", "--every", "1h", "--cmd", "true")
        self.add("empty", "--every", "1h", "--cmd", "true")
        cur = k.now() - dt.timedelta(hours=2)
        for job_id, total in (("busy", 55), ("quiet", 1)):
            for i in range(total):
                rec = k.new_run_record(
                    k.load_job(job_id), "manual", cur + dt.timedelta(seconds=i)
                )
                rec.update(
                    host="previous-machine",
                    status="success",
                    duration_seconds=1,
                    finished_at=k.iso(cur + dt.timedelta(seconds=i + 1)),
                )
                k.write_json(k.run_json_path(job_id, rec["run_id"]), rec)
        snap = k.op_snapshot()
        self.assertEqual(
            [len(snap["recent_runs"][i]) for i in ("busy", "quiet", "empty")],
            [50, 1, 0],
        )
        records = snap["recent_runs"]["busy"]
        self.assertEqual(records, sorted(records, key=lambda r: r["run_id"]))
        self.assertEqual(records[-1]["host"], "previous-machine")
        self.assertNotIn("env", records[-1])
        self.assertNotIn("command", records[-1])
        self.assertIsNotNone(k.parse_iso(snap["observed_at"]))

    def test_snapshot_keeps_active_run_after_many_skipped_slots(self):
        self.add("j", "--every", "1m", "--cmd", "true")
        cur = k.now() - dt.timedelta(hours=2)
        with k.store_lock():
            job = k.load_job("j")
            active = k.claim_run(job, "schedule", cur)
            k.save_job(job)
            for i in range(60):
                later = cur + dt.timedelta(minutes=i + 1)
                k.record_skip(job, "previous run still active", later, later, False)
        records = k.op_snapshot()["recent_runs"]["j"]
        self.assertEqual(len(records), 50)
        self.assertEqual(records[0]["run_id"], active["run_id"])
        self.assertEqual(records[0]["status"], "running")
        self.assertTrue(all(r["status"] == "skipped" for r in records[1:]))

    def test_completion_chart_counts_actual_execution_and_time_boundaries(self):
        end = toronto(2026, 11, 9, 13)

        def rec(status, hours, started=True):
            return {
                "status": status,
                "started_at": k.iso(end) if started else None,
                "finished_at": k.iso(end - dt.timedelta(hours=hours)),
            }

        runs = [
            rec("success", 24),
            rec("failed", 0),
            rec("timeout", 1),
            rec("interrupted", 1),
            rec("stopped", 1),
            rec("success", 25),
            rec("success", -1),
            rec("skipped", 1),
            rec("stopped", 1, False),
            {"status": "queued", "started_at": None, "finished_at": None},
            {"status": "running", "started_at": k.iso(end), "finished_at": None},
        ]
        counts = k.completion_bins({"j": runs}, k.iso(end))
        self.assertEqual(
            (len(counts), counts[0], counts[23], sum(counts)), (24, 1, 4, 5)
        )
        self.assertEqual(k.completion_bins({}, k.iso(end)), [0] * 24)

    def test_recent_runs_follow_latest_and_open_selected_without_waiting(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        app = k.TuiApp(None, 2)
        app.all_jobs = k.op_jobs()["jobs"]
        app.apply_view()
        job = app.job
        runs = [
            k.new_run_record(job, "schedule", k.now() + dt.timedelta(seconds=i))
            for i in range(4)
        ]
        runs[0]["status"] = "success"
        runs[1]["status"] = "skipped"
        runs[2]["status"] = "failed"
        app.recent_runs = {"j": runs[:3]}
        self.assertEqual(app.recent_run(job), runs[2])
        app.handle_key(ord("h"))
        self.assertEqual(app.recent_run(job), runs[1])
        app.recent_runs["j"].append(runs[3])
        self.assertEqual(app.recent_run(job), runs[1])
        segments = app.recent_segments(job, 5)  # selected run stays in the window
        self.assertEqual(k.line_text(segments), "▾ ! ▶")
        app.worker = mock.Mock()
        with mock.patch.object(
            k, "hub", side_effect=AssertionError("UI thread called hub")
        ):
            app.handle_key(10)
        self.assertEqual(app.page["run_id"], runs[1]["run_id"])
        app.page = None
        app.move_recent(100)
        self.assertNotIn("j", app.picked_runs)
        self.assertEqual(app.recent_run(job), runs[3])
        app.picked_runs["j"] = "pruned-record"
        self.assertEqual(app.recent_run(job), runs[3])
        self.assertEqual(
            k.line_text(app.recent_segments({"id": "empty"}, 20)), "no runs yet"
        )

    def test_recent_layout_keeps_identity_and_history(self):
        self.add("a-long-job-name", "--every", "1h", "--cmd", "true")
        app = k.TuiApp(None, 2)
        app.all_jobs = k.op_jobs()["jobs"]
        app.apply_view()
        for width in (48, 60, 80, 100, 150, 220):
            columns = app.layout(width)
            self.assertTrue(
                {"id", "host", "recent"}.issubset({key for _, key, _ in columns})
            )
            used = sum(w for _, _, w in columns) + k.TUI_GAP * (len(columns) - 1)
            self.assertLessEqual(used, width - 4)
            self.assertTrue(all(w > 0 for _, _, w in columns))

    def test_dashboard_renders_before_first_snapshot_and_with_no_jobs(self):
        screen = mock.Mock(**{"getmaxyx.return_value": (24, 80)})
        app = k.TuiApp(screen, 2)
        app.draw()  # loading starts before there are any jobs to size columns
        self.assertTrue(
            any(
                "reading the hub" in call.args[2]
                for call in screen.addnstr.call_args_list
            )
        )
        screen.reset_mock()
        app.apply(
            ("reload",),
            (
                {
                    "jobs": [],
                    "hosts": [],
                    "unsent_alerts": [],
                    "recent_runs": {},
                    "observed_at": k.iso(k.now()),
                },
                None,
                None,
            ),
            None,
        )
        app.draw()
        self.assertTrue(
            any("no jobs yet" in call.args[2] for call in screen.addnstr.call_args_list)
        )

    def test_reduced_motion_keeps_data_refresh_and_static_indicators(self):
        args = k.build_parser().parse_args(["tui", "--reduced-motion"])
        app = k.TuiApp(None, 2, args.reduced_motion)
        app.worker = mock.Mock()
        self.assertFalse(app.animating())
        self.assertEqual(app.spinner(), "●")
        app.reload()
        app.worker.submit.assert_called_once()

    def test_tui_sort_and_search(self):
        self.add("beta", "--every", "1h", "--cmd", "true", "--description", "nightly")
        self.add("alpha", "--cron", "@daily", "--cmd", "echo hello")
        self.add("gamma", "--every", "2h", "--cmd", "true", "--description", "Nightly")
        self.cli("disable", "alpha")
        app = k.TuiApp(None, 1.0)
        app.all_jobs = k.op_jobs()["jobs"]
        app.apply_view()
        self.assertEqual([j["id"] for j in app.jobs], ["alpha", "beta", "gamma"])
        app.sort = "next"  # a disabled job has no next run and sorts last
        app.apply_view()
        self.assertEqual([j["id"] for j in app.jobs], ["beta", "gamma", "alpha"])
        app.selected_id = "alpha"
        for key in map(ord, "/night"):  # smartcase: matches both descriptions
            app.handle_key(key)
        self.assertEqual(app.selected_id, "beta")  # the first match, wrapping
        app.handle_key(10)
        self.assertEqual(app.query, "night")
        app.handle_key(ord("n"))
        self.assertEqual(app.selected_id, "gamma")
        app.handle_key(ord("n"))
        self.assertEqual(app.selected_id, "beta")
        app.handle_key(ord("N"))
        self.assertEqual(app.selected_id, "gamma")
        for key in map(ord, "/Night"):  # an uppercase letter: case-sensitive
            app.handle_key(key)
        app.handle_key(10)
        self.assertEqual(app.job_matches(), [1])
        app.handle_key(27)  # Esc clears the search
        self.assertEqual(app.query, "")
        app.handle_key(ord("/"))
        app.handle_key(ord("x"))
        app.handle_key(27)  # Esc while typing goes back where it started
        self.assertEqual((app.mode, app.selected_id), (None, "gamma"))

    def test_tui_log_search(self):
        app = k.TuiApp(None, 1.0)
        app.screen = mock.Mock(**{"getmaxyx.return_value": (14, 80)})  # 10 rows
        lines = [
            f"line {i}" + (" error" if i in (3, 30, 60) else "") for i in range(80)
        ]
        app.open_pager("log", "static", [k.log_segments(line) for line in lines])
        for key in map(ord, "/error"):
            app.handle_key(key)
        app.handle_key(10)
        self.assertEqual(app.current_match(), (3, 7, 12))
        app.handle_key(ord("n"))
        self.assertEqual(app.current_match()[0], 30)
        self.assertTrue(app.pager["scroll"] <= 30 < app.pager["scroll"] + 10)
        app.handle_key(ord("n"))
        app.handle_key(ord("n"))  # wraps to the first
        self.assertEqual(app.current_match()[0], 3)
        app.handle_key(ord("N"))  # and back to the last
        self.assertEqual(app.current_match()[0], 60)
        self.assertEqual(
            k.mark([("ab", "faint"), ("cdef", "text")], [(1, 3, True)]),
            [("a", "faint"), ("b", "match_now"), ("c", "match_now"), ("def", "text")],
        )

    def test_log_lines_are_stamped_as_they_begin(self):
        out = io.BytesIO()
        log = k.StampedLog(out)
        for chunk in (b"one\ntw", b"o\n", b"", b"thr", b"ee"):
            log.write(chunk)
        lines = out.getvalue().decode().split("\n")
        self.assertEqual(len(lines), 3)
        self.assertEqual(unstamp(out.getvalue().decode()), "one\ntwo\nthree")
        stamp, text = k.split_log_line(lines[0])
        self.assertRegex(stamp, r"^\d\d:\d\d:\d\d$")
        self.assertEqual(text, "one")
        self.assertEqual(k.split_log_line("plain"), ("", "plain"))

    def test_tui_run_asks_first(self):
        self.add("beta", "--every", "1h", "--cmd", "true")
        app = k.TuiApp(None, 1.0)
        app.all_jobs = k.op_jobs()["jobs"]
        app.apply_view()
        app.handle_key(ord("r"))
        self.assertEqual(app.mode, "confirm")
        app.handle_key(ord("n"))  # anything but y cancels
        self.assertIsNone(app.mode)
        self.assertEqual(k.op_runs("beta", False, 0), [])

    def test_tui_parser_and_noninteractive_rejection(self):
        args = k.build_parser().parse_args(["ui", "--refresh", "0.5"])
        self.assertEqual(args.command, "ui")
        self.assertEqual(args.refresh, 0.5)
        with mock.patch("sys.stdin.isatty", return_value=False):
            with self.assertRaises(k.KoyomiError):
                k.cmd_tui(args)

    def test_init_reads_smtp_settings(self):
        env = self.root / "smtp.env"
        env.write_text(
            "AWS_SECRET_ACCESS_KEY=nope\nSMTP_HOST=h\nSMTP_PORT=587\nSMTP_USER=u\nSMTP_PASSWORD='p w'\n"
        )
        self.assertEqual(
            self.cli(
                "init",
                "box",
                "--hub-local",
                "--always-on",
                "--smtp-env",
                str(env),
                "--email-from",
                "a@x",
                "--email-to",
                "b@x",
            ),
            0,
        )
        cfg = k.load_config()
        self.assertEqual(cfg["email"]["smtp_password"], "p w")
        self.assertNotIn("nope", k.config_path().read_text())
        self.assertEqual(k.config_path().stat().st_mode & 0o777, 0o600)


class FixTests(HubCase):
    """`koyomi fix` with the foreground process launch intercepted."""

    def setUp(self):
        super().setUp()
        self.launch_dir = (self.root / "launch  directory").resolve()
        self.launch_dir.mkdir()
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self.launch_dir)
        self.launches = []
        which = mock.patch.object(
            k.shutil,
            "which",
            side_effect=lambda name: "/fake/bin/codex" if name == "codex" else None,
        )
        launch = mock.patch.object(
            os,
            "execvp",
            side_effect=lambda program, argv: self.launches.append(
                (Path.cwd(), program, argv)
            ),
        )
        for patcher in (which, launch):
            patcher.start()
            self.addCleanup(patcher.stop)

    def fix_argv(self, *args, cwd=None):
        before = len(self.launches)
        self.assertEqual(self.cli("fix", *args), 0)
        self.assertEqual(len(self.launches), before + 1)
        actual_cwd, program, argv = self.launches[-1]
        self.assertEqual(actual_cwd, cwd or self.launch_dir)
        self.assertEqual(program, argv[0])
        return argv

    def make_failing(self, job_id):
        self.add(job_id, "--every", "1h", "--cwd", str(self.root), "--cmd", "exit 3")
        self.set_next_run(job_id, k.now())
        self.tick()

    def test_one_failing_job_gets_an_agent_in_the_callers_directory(self):
        self.add("fine", "--every", "1h", "--cmd", "true")
        self.make_failing("bad")
        argv = self.fix_argv("-m", "check\nthe  token")
        self.assertEqual(argv[:2], k.FIX_AGENTS["codex"])  # the only one installed
        self.assertIn("Fix the Koyomi job bad on box (last run", argv[-1])
        self.assertIn('"Fixing failed jobs"', argv[-1])
        self.assertTrue(argv[-1].endswith("Guidance from the user: check the token"))

    def test_project_fix_uses_the_worktree_root_to_scope_jobs(self):
        (self.launch_dir / ".git").write_text("gitdir: /worktrees/project\n")
        nested = self.launch_dir / "worker"
        nested.mkdir()
        os.chdir(nested)
        self.add("worker", "--every", "1h", "--cwd", str(nested), "--cmd", "true")
        self.add(
            "cleanup", "--every", "1h", "--cwd", str(self.launch_dir), "--cmd", "true"
        )
        k.op_register_host("remote", True, sys.platform)
        self.add(
            "remote-job",
            "--host", "remote",
            "--every", "1h",
            "--cwd", str(nested),
            "--cmd", "true",
        )
        self.make_failing("unrelated")

        argv = self.fix_argv(cwd=nested)
        self.assertEqual(argv[:-1], k.FIX_AGENTS["codex"])
        self.assertIn(str(self.launch_dir), argv[-1])
        self.assertIn("AGENTS.md", argv[-1])
        self.assertIn("cleanup, worker", argv[-1])
        self.assertNotIn("unrelated", argv[-1])
        self.assertNotIn("remote-job", argv[-1])

        prompt = self.fix_argv("unrelated", cwd=nested)[-1]
        self.assertIn("Fix the Koyomi job unrelated", prompt)

    def test_project_fix_starts_even_without_jobs(self):
        (self.launch_dir / ".git").mkdir()
        prompt = self.fix_argv()[-1]
        self.assertIn('"Fixing a project"', prompt)

    def test_several_jobs_get_an_orchestrator(self):
        self.make_failing("a")
        self.make_failing("b")
        argv = self.fix_argv("--agent", "opencode")
        self.assertEqual(argv[:-1], ["opencode", "--auto", "--prompt"])
        self.assertIn("orchestrator for these Koyomi jobs: a on box", argv[-1])
        self.assertIn("ask the user which plan", argv[-1])

    def test_orchestrator_runs_on_a_low_effort_model(self):
        self.make_failing("a")
        self.make_failing("b")
        argv = self.fix_argv("--agent", "claude")
        self.assertEqual(
            argv[:-1],
            [
                "claude",
                "--model",
                "claude-opus-5-5",
                "--effort",
                "low",
                "--dangerously-skip-permissions",
            ],
        )
        argv = self.fix_argv("--agent", "claude", "a")
        self.assertEqual(argv[:-1], k.FIX_AGENTS["claude"])

    def test_disabled_jobs_are_not_failing(self):
        self.make_failing("off")
        self.cli("disable", "off")
        self.assertEqual(self.cli("fix"), 0)
        self.assertEqual(self.launches, [])

    def test_nothing_failing_starts_nothing(self):
        self.add("fine", "--every", "1h", "--cmd", "true")
        self.assertEqual(self.cli("fix"), 0)
        self.assertEqual(self.launches, [])
        self.assertEqual(self.cli("fix", "nope"), 1)


if __name__ == "__main__":
    unittest.main()
