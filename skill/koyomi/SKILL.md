---
name: koyomi
description: Schedule, inspect and debug local jobs on this machine with the `koyomi` CLI (cron-like recurring jobs, one-time jobs, run history, logs). Use when the user wants something to run later, on a schedule, every N minutes/hours/days, at a specific time, or asks about scheduled/background jobs, cron, launchd timers, or why a scheduled job failed.
---

# Koyomi

Koyomi is a small local job scheduler. A launchd agent (`koyomi daemon`) runs jobs; all
state is plain JSON in `~/.koyomi/`. Always operate it through the `koyomi` CLI (on PATH).

## Workflow

1. `koyomi status`: check that the scheduler is running. If not: `koyomi service start`
   (or `koyomi service install` if the agent is missing).
2. Create the job with an explicit id, schedule, command and working directory.
3. Test it right away with `koyomi run <id>`. This runs in the foreground and streams output.
   Scheduled runs use the same environment the scheduler was installed with, so prefer
   absolute paths for scripts and tools.
4. Confirm with `koyomi show <id>`, which shows the next run times.
5. Tell the user the job id, schedule, next run time, and how to check it (`koyomi logs <id>`).

## Creating jobs

```bash
# recurring, standard 5-field cron in local time (minute hour day month weekday)
koyomi add notes-backup --cron "30 2 * * *" --cwd ~/obsidian --cmd "git add -A && git commit -qm backup || true"
koyomi add weekday-report --cron "0 9 * * mon-fri" -- /usr/local/bin/report --quiet
koyomi add sync --every 15m --cmd "./sync.sh" --cwd ~/pro/myproj --timeout 10m --notify
# one-time
koyomi add remind --at "2026-09-20 14:00" --cmd 'osascript -e "display notification \"Call Sam\""'
koyomi add later --in 2h --cmd "python3 cleanup.py"
```

- Schedule: exactly one of `--cron`, `--every` (min 1m), `--at` ("YYYY-MM-DD HH:MM" or "HH:MM"), `--in`.
  `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` also work.
- `--cmd "..."` runs through `/bin/sh -c`, so pipes, `&&` and redirects work. Or put argv after `--`.
- `--cwd` defaults to the **current directory**. Set it explicitly when you are not already in the right place.
- `--env KEY=VALUE` (repeatable), `--timeout 30m`, `--notify` (macOS notification on failure),
  `--description "..."`, `--disabled`.
- `--catchup once` (default): if the laptop was asleep or off when the job was due, run it once
  on wake. `--catchup skip`: drop slots that are more than 5 minutes late.
- Job ids: letters, digits, `.`, `_` and `-`. Jobs get `KOYOMI_JOB` and `KOYOMI_RUN_ID` env vars.

## Managing jobs

```bash
koyomi list                     # id, state, schedule, next run, last result
koyomi show <id>                # full details + upcoming times   (--json for raw)
koyomi update <id> --cron "0 8 * * *"   # only the flags you pass change
koyomi update <id> --timeout none --no-notify --unset-env FOO
koyomi disable <id> / koyomi enable <id>   # re-enabling does not back-fill missed runs
koyomi delete <id>              # also deletes run history unless --keep-logs
koyomi run <id>                 # run now in foreground; exit code = job's exit code
koyomi run <id> --detach        # run now in background
koyomi stop <id>                # interrupt the active run (records it as "interrupted")
```

## Debugging

```bash
koyomi status                   # scheduler health + jobs whose last run failed
koyomi history [<id>] [--failed] [-n 50] [--json]
koyomi logs <id>                # output of latest run (--run RUN_ID for a specific one, -n 0 = all)
koyomi logs --daemon            # scheduler events: starts, skips, catch-ups, interruptions
koyomi tui                      # interactive dashboard (for the user, not for agents)
```

Run statuses: `running`, `success`, `failed` (non-zero exit), `timeout`, `interrupted`
(the runner died, e.g. after a reboot) and `skipped` (the previous run was still going, or the slot was missed with catchup=skip).
A job never overlaps itself.

Common failures: `command not found` (use an absolute path, or check the PATH shown in
`~/Library/LaunchAgents/local.koyomi.scheduler.plist`), a wrong `cwd`, or scripts that expect
an interactive shell. Reproduce with `koyomi run <id>`.

## Rules

- Do not hand-edit `~/.koyomi/` JSON while jobs may be running. Use `koyomi update`.
- Do not create launchd plists or crontab entries for tasks Koyomi can handle.
- Ask before deleting jobs you did not create.
- Jobs run only while the user is logged in and the machine is awake. Missed runs catch up on wake.
