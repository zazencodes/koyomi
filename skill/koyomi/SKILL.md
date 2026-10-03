---
name: koyomi
description: Schedule, inspect and debug jobs on Alex's machines (this Mac and the VPS emanator) with the `koyomi` CLI (cron-like recurring jobs, one-time jobs, run history, logs, failure alerts by email). Use when the user wants something to run later, on a schedule, every N minutes/hours/days, at a specific time, in the cloud, on the server/VPS or on this machine, wants a cloud task or remote job set up, or asks about scheduled/background jobs, cron, launchd or systemd timers, why a scheduled job failed, or deploying/updating Koyomi itself.
---

# Koyomi

Koyomi is a small job scheduler for a few machines. The hub (`emanator`) keeps every job, run
record and log; every job is pinned to a host (`mac` or `emanator`) whose scheduler runs it. The
`koyomi` CLI works the same on every machine and shows every job on every host. Always operate it
through the CLI (on PATH).

- `emanator` is always on: jobs run on schedule; a late or missed slot emails an alert.
- `mac` runs jobs only while awake and online; a slot missed while asleep runs once on wake.
- A failed, timed-out or interrupted run emails an alert, once per failure streak (quiet until the
  job succeeds again). There is nothing to opt into.

## Workflow

1. `koyomi status`: check the hosts are up and the local scheduler is running. If the local one is
   not: `koyomi service start` (or `./install.sh` in the koyomi repo if the service is missing).
2. Create the job with an explicit id, host, schedule, command and working directory.
3. Test it right away with `koyomi run <id>`. It runs on the job's host, in the scheduler's
   environment, and streams the output here; the exit code is the job's. Prefer absolute paths.
4. Confirm with `koyomi show <id>`, which shows the next run times.
5. Tell the user the job id, host, schedule, next run time, and how to check it (`koyomi logs <id>`).

## Creating jobs

```bash
# recurring, standard 5-field cron (minute hour day month weekday) in the job's time zone
koyomi add notes-backup --cron "30 2 * * *" --cwd ~/obsidian --cmd "git add -A && git commit -qm backup || true"
koyomi add nightly-report --host emanator --tz America/Toronto --cron "0 6 * * *" \
  --cwd /root/app --timeout 1h -- bin/report --quiet
koyomi add sync --every 15m --cmd "./sync.sh" --cwd ~/pro/myproj --timeout 10m
# one-time
koyomi add later --in 2h --cmd "python3 cleanup.py"
koyomi add remind --at "2026-09-20 14:00" --cmd 'osascript -e "display notification \"Call Sam\""'
```

- `--host` defaults to this machine. A job on another host needs `--cwd` as an absolute path on that host.
- `--tz` (IANA name) sets the zone for `--cron` and `--at`; it defaults to this machine's zone.
  emanator's clock is UTC, so pass `--tz America/Toronto` for Toronto times there.
- Schedule: exactly one of `--cron`, `--every` (min 1m), `--at` ("YYYY-MM-DD HH:MM" or "HH:MM"), `--in`.
  `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` also work.
- `--cmd "..."` runs through `/bin/sh -c`, so pipes, `&&` and redirects work. Or put argv after `--`.
- `--env KEY=VALUE` (repeatable), `--timeout 30m`, `--description "..."`, `--disabled`.
- `--catchup once` (default): a slot missed while the host was down or asleep runs once when it is
  back. `--catchup skip`: drop slots that are more than 5 minutes late.
- Job ids: letters, digits, `.`, `_` and `-`. Jobs get `KOYOMI_JOB`, `KOYOMI_RUN_ID` and `KOYOMI_HOST`.

## Jobs on emanator (cloud tasks)

Koyomi only stores and runs the command. Whatever it runs must already be on emanator, so set
that up first, over `ssh emanator` (Ubuntu 24.04, user `root`):

1. **Code:** clone the repo under `/root` (`gh` is logged in as `zazencodes` and is git's credential
   helper): `ssh emanator 'cd /root && gh repo clone zazencodes/<repo>'`. Install its dependencies
   there. Keep it current by having the job pull first (as `/root/agent-signals/bin/scheduled-run`
   does), or by pulling by hand.
2. **Secrets:** copy them as a file the job reads, e.g.
   `scp .env emanator:/root/<repo>/.env && ssh emanator chmod 600 /root/<repo>/.env`. Don't pass
   secrets with `--env`: they are stored in plain JSON on the hub and shown by `koyomi show`.
3. **Check it by hand** with the scheduler's environment before scheduling:
   `ssh emanator 'cd /root/<repo> && <command>'`.
4. **Add it from any machine:** `koyomi add <id> --host emanator --cwd /root/<repo> --tz America/Toronto
   --cron "..." --timeout <limit> -- <command>`, then `koyomi run <id>` to test it end to end.

What jobs on emanator get:

- User `root`, `HOME=/root`, and the PATH baked into `/etc/systemd/system/koyomi.service`: it has
  `/root/.local/bin` (uv, koyomi), `/usr/local/bin` (npm globals such as `codex`) and `/usr/bin`
  (node 24, git, gh, python3.12). Nothing from `~/.bashrc` is loaded.
- `--cmd` runs with `/bin/sh`, which is **dash** there: no bash-isms (`source`, `[[ ]]`, arrays).
  Put anything non-trivial in a script with a `#!/usr/bin/env bash` line and run that.
- `--cwd` can't be checked from the Mac; a wrong one fails the first run ("could not start command").
- Always set `--timeout`: an always-on host has no one watching it.
- Restarting or reinstalling Koyomi does not kill runs in progress.

## Managing jobs

```bash
koyomi list                     # id, host, state, schedule, next run, last result
koyomi show <id>                # full details + upcoming times   (--json for raw)
koyomi update <id> --cron "0 8 * * *"   # only the flags you pass change (also --host, --tz)
koyomi update <id> --timeout none --unset-env FOO
koyomi disable <id> / koyomi enable <id>   # re-enabling does not back-fill missed runs
koyomi delete <id>              # also deletes run history unless --keep-logs
koyomi run <id>                 # run now on its host, stream output; Ctrl-C stops the run
koyomi run <id> --detach        # queue it and return
koyomi stop <id>                # stop the active run (recorded as "stopped", no alert)
```

## Debugging

```bash
koyomi status                   # hosts, scheduler health, failing jobs, unsent alerts
koyomi history [<id>] [--failed] [-n 50] [--json]
koyomi logs <id>                # output of latest run (--run RUN_ID for a specific one, -n 0 = all)
koyomi logs --daemon            # scheduling events from every host: starts, skips, catch-ups, alerts
koyomi tui                      # interactive dashboard (for the user, not for agents)
```

Run statuses: `queued`, `running`, `success`, `failed` (non-zero exit), `timeout`, `stopped`
(by request), `interrupted` (the runner died, e.g. after a reboot) and `skipped` (the previous run
was still going, or the slot was missed with catchup=skip). A job never overlaps itself.

Common failures: `command not found` (use an absolute path, or check the PATH baked into the
service: `~/Library/LaunchAgents/local.koyomi.scheduler.plist` on the Mac,
`/etc/systemd/system/koyomi.service` on emanator), a wrong `cwd`, or scripts that expect an
interactive shell. Reproduce with `koyomi run <id>`. Scheduler process errors are in
`~/.koyomi/launchd.log` (Mac) and `journalctl -u koyomi` (emanator).

## Koyomi itself

- **Source:** `~/pro/koyomi` (github.com/zazencodes/koyomi). Its `AGENTS.md` is the full guide
  for changing and deploying it; read it before touching Koyomi's code or services.
- **Hub:** emanator holds all state in `/root/.koyomi/`. The scheduler is the systemd unit `koyomi`, and the
  installed checkout is `/root/koyomi`, an rsync copy rather than a git clone.
- **Mac:** a host that reaches the hub with `ssh emanator /root/.local/bin/koyomi _rpc`; its
  `~/.koyomi/config.json` holds that command. If every command fails with "cannot reach the hub",
  check `ssh emanator true` first.
- **Deploying a change** (hub first; both machines must run the same version, or every call fails
  with "version mismatch"):
  ```bash
  rsync -a --delete --exclude .git --exclude .venv --exclude build --exclude '*.egg-info' \
    --exclude __pycache__ --exclude .ruff_cache ~/pro/koyomi/ emanator:/root/koyomi/
  ssh emanator 'cd /root/koyomi && PATH=/root/.local/bin:$PATH ./install.sh'
  cd ~/pro/koyomi && ./install.sh
  ```
- **A new host:** see "Install" in the repo's `README.md` (`uv tool install .`, `koyomi init`, `./install.sh`).

## Rules

- Do not hand-edit `~/.koyomi/` JSON on the hub. Use `koyomi update`.
- Do not create launchd plists, systemd timers or crontab entries for tasks Koyomi can handle.
- Ask before deleting jobs you did not create.
