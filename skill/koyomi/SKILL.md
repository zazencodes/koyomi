---
name: koyomi
description: Schedule, inspect and debug jobs on one or more machines with the `koyomi` CLI (cron-like recurring jobs, one-time jobs, run history, logs, failure alerts by email). Use when the user wants something to run later, on a schedule, every N minutes/hours/days, at a specific time, in the cloud, on the server/VPS or on this machine, wants a cloud task or remote job set up, or asks about scheduled/background jobs, cron, launchd or systemd timers, why a scheduled job failed, wants failed jobs fixed, was started by `koyomi fix`, or deploying/updating Koyomi itself.
---

# Koyomi

Koyomi is a small job scheduler for a few machines. The hub keeps every job, run record and
log. Each job runs on its assigned host. Use the `koyomi` CLI to operate the scheduler.

## Find the site's hosts

Read `site-notes.md` in the Koyomi state directory when it exists. The directory is
`${KOYOMI_HOME:-$HOME/.koyomi}`. The operator keeps this private file outside the repo and
records host roles, SSH destinations, checkout paths, time zones and deployment procedures there.
Keep credentials in the site's credential store.

Use `koyomi status` to verify the hub, registered hosts, and service and alert status. For an existing
job, use `koyomi show <id>` to confirm which host runs it. Host IDs may differ from SSH destinations.
Get any access details missing from the site notes from the operator.

- Always-on hosts run jobs on schedule. A late or missed slot can trigger an email alert.
- When-awake hosts run jobs while awake and online. Missed slots catch up once by default.
- With SMTP configured on the hub, a failed, timed-out or interrupted run can email an alert
  once per failure streak. Other hosts use their own SMTP configuration to report hub outages.

## Workflow

1. Run `koyomi status`. Check that the target host is reporting and verify the local scheduler and
   email alert status. Use `koyomi service start` if the local scheduler needs starting.
2. Create the job with an explicit id, host, schedule, command and working directory.
3. Test it right away with `koyomi run <id>`. It runs on the job's host, in the scheduler's
   environment, and streams the output here; the exit code is the job's. Prefer absolute paths.
4. Confirm with `koyomi show <id>`, which shows the next run times.
5. Tell the user the job id, host, schedule, next run time, and how to check it (`koyomi logs <id>`).

## Creating jobs

```bash
# recurring, standard 5-field cron (minute hour day month weekday) in the job's time zone
koyomi add notes-backup --cron "30 2 * * *" --cwd ~/work/notes --cmd "./backup.sh"
koyomi add nightly-report --host server --tz Europe/Berlin --cron "0 6 * * *" \
  --cwd /srv/report --timeout 1h -- bin/report --quiet
koyomi add sync --every 15m --cmd "./sync.sh" --cwd ~/work/myproj --timeout 10m
# one-time
koyomi add later --in 2h --cmd "python3 cleanup.py"
koyomi add remind --at "14:00" --cmd "echo reminder"
```

- `--host` defaults to this machine. A job on another host needs `--cwd` as an absolute path on that host.
- `--tz` (IANA name) sets the zone for `--cron` and `--at`; it defaults to this machine's zone.
  Set it explicitly when the schedule should follow a particular zone.
- Schedule: exactly one of `--cron`, `--every` (min 1m), `--at` ("YYYY-MM-DD HH:MM" or "HH:MM"), `--in`.
  `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly` also work.
- `--cmd "..."` runs through `/bin/sh -c`, so pipes, `&&` and redirects work. Or put argv after `--`.
- `--env KEY=VALUE` (repeatable), `--timeout 30m`, `--description "..."`, `--disabled`.
- `--catchup once` (default): a slot missed while the host was down or asleep runs once when it is
  back. `--catchup skip`: drop slots that are more than 5 minutes late.
- Job ids: letters, digits, `.`, `_` and `-`. Jobs get `KOYOMI_JOB`, `KOYOMI_RUN_ID` and `KOYOMI_HOST`.

## Jobs on a remote host

Prepare the job on its target host before scheduling it. Get the SSH destination, checkout path
and deployment steps from the site notes or the operator.

1. Put the code and dependencies on the target host.
2. If the job needs secrets, install a private file on that host (mode 600) and have the job read it.
   Koyomi stores `--env` values in plain JSON on the hub and displays them in `koyomi show`.
3. Check the command on the host using the scheduler's working directory, shell and PATH.
4. Add the job with `--host <host-id>`, an absolute `--cwd` on that host, the intended time zone
   and an explicit timeout. Use `koyomi run <id>` for an end-to-end test when the command is safe to run.

The service captures its install-time `HOME` and `PATH` and `--cmd` runs through `/bin/sh`.
Use a script with the appropriate shebang when the command needs a different shell.

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
koyomi status                   # hosts, scheduler health, failing jobs, email alert status
koyomi history [<id>] [--failed] [-n 50] [--json]
koyomi logs <id>                # output of latest run, lines prefixed with local HH:MM:SS (--run RUN_ID, -n 0 = all)
koyomi logs --daemon            # scheduling events from every host: starts, skips, catch-ups, alerts
koyomi tui                      # interactive dashboard (for the user, not for agents)
koyomi fix [ID...] [-a claude|codex|pi|opencode] [-m "guidance"]   # fix this project or target jobs
```

Run statuses: `queued`, `running`, `success`, `failed` (non-zero exit), `timeout`, `stopped`
(by request), `interrupted` (the runner died, e.g. after a reboot) and `skipped` (the previous run
was still going, or the slot was missed with catchup=skip). A job never overlaps itself.

Common failures: `command not found` (use an absolute path, or check the PATH baked into the
service: `~/Library/LaunchAgents/local.koyomi.scheduler.plist` on macOS,
`/etc/systemd/system/koyomi.service` on Linux), a wrong `cwd`, or scripts that expect an
interactive shell. Reproduce with `koyomi run <id>`. Scheduler process errors are in
`~/.koyomi/launchd.log` (macOS) and `journalctl -u koyomi` (Linux).

## Fixing a project

In a Git checkout, bare `koyomi fix` starts an agent in the caller's terminal and directory. It
gives the agent the repository root and the local Koyomi jobs with directories inside the checkout.

1. Read the repository's `AGENTS.md` first. Follow its instructions and start diagnosing directly.
2. Use `koyomi show <id>`, `koyomi history <id> -n 20`, and `koyomi logs <id> --run <RUN_ID>` for
   the project's jobs. Check for application errors even when a run exits successfully.
3. Trace the evidence through the project's code, logs and checks. Investigate the project even
   when no jobs are listed. Fix the cause and verify with the project's checks. Apply the rerun
   guidance below before starting any scheduled command.

## Fixing failed jobs

`koyomi fix <id>...` targets the named jobs. Outside a Git checkout, bare `koyomi fix` targets all
failing jobs. The agent starts in the caller's terminal and directory. A prompt for one job makes
you its fixer. A prompt to orchestrate several jobs makes you their orchestrator.

**Fixer**, for each job:

1. Gather the facts: `koyomi show <id>`, `koyomi history <id> -n 20` (since when? every run or
   some?), `koyomi logs <id> -n 0`, and `koyomi logs <id> --run <RUN_ID>` to compare an earlier
   success. For `interrupted` or scheduling problems, `koyomi logs --daemon`.
2. Find the cause where the job runs. A job on this host: work in its `cwd`. A job on another host:
   use the SSH destination in the site notes. If the host is inaccessible, report what the logs show
   and where the fix needs to be made. Reproduce with the scheduler's PATH and `/bin/sh`.
3. Fix the root cause: the job's code, its definition (`koyomi update`), or its environment
   (a missing tool, secret or login). A one-off cause (network blip, an `interrupted` run after a
   reboot or sleep) needs no fix; say so.
4. Rerun with `koyomi run <id>` only when it is worth it: it is the real job, so skip it when a
   second run would repeat side effects (emails, posts, payments, deploys) or take very long, and
   use `--detach` for long runs. A successful run ends the job's failure streak.
5. Leave changes uncommitted unless the user says to commit, push or deploy. Ask before disabling,
   deleting or loosening a job (timeout, `|| true`) just to make it pass.
6. Report per job: the cause, what you changed, whether a rerun passed, what the user must do.

**Orchestrator:** you run at low effort (Claude: `claude-opus-5-5`, `--effort low`; Codex:
`gpt-6.1-sol`, `model_reasoning_effort="low"`; pi and opencode: their defaults), so plan before
you fix. Project and single-job fixers use the agent's defaults.

1. Triage every job (fixer steps 1-2, briefly). Jobs often share a cause (an expired login, a
   down host, a broken PATH) that one fix covers. Judge how hard each job is to fix.
2. Propose which jobs to fix and in what order. Wait for the user's answer before changing anything.
3. Follow the agreed plan and report the outcome for each job.

## Koyomi itself

Read the source checkout's `AGENTS.md` before editing Koyomi and its `README.md` before
installing it on a new host. Consult the private site notes for checkout locations and deployment
steps. When deploying, update the hub first, then every other host. The CLI, schedulers and hub
must all run the same version.

## Rules

- Do not hand-edit `~/.koyomi/` JSON on the hub. Use `koyomi update`.
- Do not create launchd plists, systemd timers or crontab entries for tasks Koyomi can handle.
- Ask before deleting jobs you did not create.
