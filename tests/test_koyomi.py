import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import koyomi as k  # noqa: E402


def local(*args):
    return dt.datetime(*args).astimezone()


class CronTests(unittest.TestCase):
    def test_basic(self):
        c = k.Cron("30 9 * * *")
        self.assertEqual(c.next_after(local(2026, 9, 13, 9, 29)), local(2026, 9, 13, 9, 30))
        self.assertEqual(c.next_after(local(2026, 9, 13, 9, 30)), local(2026, 9, 14, 9, 30))

    def test_steps_ranges_names(self):
        c = k.Cron("*/15 9-17 * * mon-fri")
        # 2026-09-13 is a Sunday
        self.assertEqual(c.next_after(local(2026, 9, 13, 12, 0)), local(2026, 9, 14, 9, 0))
        self.assertEqual(c.next_after(local(2026, 9, 14, 9, 7)), local(2026, 9, 14, 9, 15))
        self.assertEqual(c.next_after(local(2026, 9, 14, 17, 45)), local(2026, 9, 15, 9, 0))

    def test_dom_dow_or(self):
        c = k.Cron("0 0 1 * sun")  # 1st of month OR Sunday
        self.assertEqual(c.next_after(local(2026, 9, 13, 1, 0)), local(2026, 9, 20, 0, 0))
        self.assertEqual(c.next_after(local(2026, 9, 27, 1, 0)), local(2026, 10, 1, 0, 0))

    def test_aliases_and_errors(self):
        self.assertEqual(k.Cron("@daily").next_after(local(2026, 1, 1, 5)), local(2026, 1, 2))
        self.assertEqual(k.Cron("0 0 29 2 *").next_after(local(2026, 3, 1)), local(2028, 2, 29))
        for bad in ("* * *", "60 * * * *", "* * * * 8", "a b c d e"):
            with self.assertRaises(k.KoyomiError):
                k.Cron(bad)
        with self.assertRaises(k.KoyomiError):
            k.Cron("0 0 31 2 *").next_after(local(2026, 1, 1))

    def test_duration(self):
        self.assertEqual(k.parse_duration("1h30m"), 5400)
        self.assertEqual(k.parse_duration("45s"), 45)
        with self.assertRaises(k.KoyomiError):
            k.parse_duration("5 minutes")


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["KOYOMI_HOME"] = self.tmp.name
        self.out = Path(self.tmp.name) / "out.txt"

    def tearDown(self):
        os.environ.pop("KOYOMI_HOME")
        self.tmp.cleanup()

    def add(self, *args):
        with open(os.devnull, "w") as devnull:
            stdout, stderr = sys.stdout, sys.stderr
            sys.stdout = sys.stderr = devnull
            try:
                return k.main(["add", *args])
            finally:
                sys.stdout, sys.stderr = stdout, stderr

    def set_next_run(self, job_id, t):
        job = k.load_job(job_id)
        job["next_run"] = k.iso(t)
        k.write_json(k.job_path(job_id), job)

    def tick(self, cur):
        procs, _ = k.tick(cur)
        for p in procs:
            p.wait()
        return procs

    def test_missed_slots_collapse_to_one_catchup_run(self):
        self.add("j", "--cron", "0 * * * *", "--cmd", f"echo ran >> {self.out}")
        cur = k.now()
        self.set_next_run("j", cur - dt.timedelta(hours=10))  # asleep for 10 slots
        self.assertEqual(len(self.tick(cur)), 1)
        self.assertEqual(self.out.read_text(), "ran\n")
        job = k.load_job("j")
        self.assertGreater(k.parse_iso(job["next_run"]), cur)
        self.assertEqual(job["last_run"]["status"], "success")
        self.assertIsNone(job["running"])
        # a second tick at the same moment must not run again
        self.assertEqual(len(self.tick(cur)), 0)
        self.assertEqual(self.out.read_text(), "ran\n")

    def test_catchup_skip(self):
        self.add("j", "--every", "1h", "--catchup", "skip", "--cmd", f"echo ran >> {self.out}")
        cur = k.now()
        self.set_next_run("j", cur - dt.timedelta(hours=3))
        self.assertEqual(len(self.tick(cur)), 0)
        self.assertFalse(self.out.exists())
        self.assertEqual(k.load_job("j")["last_run"]["status"], "skipped")
        # but a slot that is only slightly late still runs
        self.set_next_run("j", cur - dt.timedelta(seconds=20))
        self.assertEqual(len(self.tick(cur)), 1)

    def test_no_overlap_while_running(self):
        self.add("j", "--every", "1m", "--cmd", "sleep 3")
        cur = k.now()
        self.set_next_run("j", cur)
        procs, _ = k.tick(cur)
        self.assertEqual(len(procs), 1)
        self.set_next_run("j", cur)
        procs2, _ = k.tick(cur)
        self.assertEqual(procs2, [])
        statuses = sorted(r["status"] for r in k.load_runs("j"))
        self.assertEqual(statuses, ["running", "skipped"])
        procs[0].wait()
        self.assertEqual(k.load_job("j")["last_run"]["status"], "success")

    def test_one_time_job_runs_once(self):
        self.add("once", "--in", "1m", "--cmd", f"echo ran >> {self.out}")
        cur = k.now() + dt.timedelta(minutes=2)
        self.assertEqual(len(self.tick(cur)), 1)
        self.assertIsNone(k.load_job("once")["next_run"])
        self.assertEqual(len(self.tick(cur + dt.timedelta(days=1))), 0)
        self.assertEqual(self.out.read_text(), "ran\n")

    def test_failure_and_timeout_recorded(self):
        self.add("bad", "--every", "1h", "--cmd", "echo boom; exit 3")
        self.add("slow", "--every", "1h", "--timeout", "1s", "--cmd", "sleep 30")
        cur = k.now()
        self.set_next_run("bad", cur)
        self.set_next_run("slow", cur)
        self.tick(cur)
        bad = k.load_runs("bad")[-1]
        self.assertEqual((bad["status"], bad["exit_code"]), ("failed", 3))
        self.assertIn("boom", bad["output_tail"])
        self.assertEqual(k.load_runs("slow")[-1]["status"], "timeout")

    def test_stale_running_marker_is_reconciled(self):
        self.add("j", "--every", "1h", "--cmd", "true")
        job = k.load_job("j")
        rec = k.claim_run(job, "schedule", k.now() - dt.timedelta(minutes=5))
        dead = subprocess.Popen(["true"])
        dead.wait()
        job["running"]["pid"] = dead.pid
        k.write_json(k.job_path("j"), job)
        k.tick(k.now())
        job = k.load_job("j")
        self.assertIsNone(job["running"])
        self.assertEqual(k.read_json(k.run_json_path("j", rec["run_id"]))["status"], "interrupted")

    def test_disabled_and_reenable_do_not_backfill(self):
        self.add("j", "--every", "1h", "--cmd", f"echo ran >> {self.out}")
        k.main(["disable", "j"])
        self.assertEqual(len(self.tick(k.now() + dt.timedelta(hours=5))), 0)
        k.main(["enable", "j"])
        self.assertGreater(k.parse_iso(k.load_job("j")["next_run"]), k.now())

    def test_json_is_plain(self):
        self.add("j", "--cron", "@daily", "--", "echo", "hello world")
        data = json.loads(k.job_path("j").read_text())
        self.assertEqual(data["command"], "echo 'hello world'")
        self.assertEqual(data["schedule"], {"cron": "@daily"})


if __name__ == "__main__":
    unittest.main()
