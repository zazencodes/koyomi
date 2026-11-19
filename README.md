# Koyomi

A small job scheduler for a few machines. One Python file (stdlib only), plain JSON state, a launchd agent on macOS and a systemd service on Linux.

One machine is the **hub**: it holds every job, run record and log in `~/.koyomi/`. Every job is pinned to a **host** that runs it. Each host runs a scheduler that asks the hub for its due runs, runs them, and streams their output back. Other machines reach the hub with `ssh <hub> koyomi _rpc`, so the CLI and the dashboard show and control every job on every host from any machine.

Hosts are either **always on** (a server: runs on schedule, and a late or missed slot can raise an email alert) or **when awake** (a laptop: runs while awake and online; a slot missed while asleep runs once on wake).

## Install

You need Python 3.10+, [uv](https://docs.astral.sh/uv/), and a checkout of this repo on each machine.
Put the directory reported by `uv tool dir --bin` on your PATH.
Start with the hub. A single-machine setup only needs the hub steps.

- **macOS:** run the commands as the logged-in user. Koyomi installs a launchd agent that starts at login.
- **Linux:** use a systemd machine and run installation and CLI commands from a root shell. Koyomi installs a system service that starts at boot and runs jobs as root. Make sure `uv` is on that shell's PATH.

On the hub, from the checkout:

```sh
uv tool install .
koyomi init server --hub-local --always-on
koyomi service install
```

Omit `--always-on` if the hub is a machine that regularly sleeps.

For additional hosts, set up key-based SSH to the hub account that installed Koyomi. From each host, verify that `ssh server true` works without a prompt. Use the absolute path reported by `command -v koyomi` on the hub in place of the example path:

```sh
uv tool install .
koyomi init laptop --hub-ssh server --hub-koyomi /opt/koyomi/bin/koyomi
koyomi service install
```

`koyomi init` saves the configuration to `~/.koyomi/config.json` (mode 600). The service captures the current shell's `PATH` and `HOME` for scheduled jobs. Check the installation with `koyomi status`.

To upgrade, run `./install.sh` from the updated checkout on the hub first, then on every host. The script reinstalls the CLI and scheduler service using the existing configuration. The hub and all hosts must run the same Koyomi version.

## Usage

```bash
koyomi add backup --cron "30 2 * * *" --cwd ~/work/notes --cmd "./backup.sh"
koyomi add report --host server --tz Europe/Berlin --cron "0 6 * * *" \
  --cwd /srv/report --timeout 3h -- bin/report daily
koyomi add remind --in 2h --cmd "echo reminder"

koyomi list                # jobs, host, next run, last result
koyomi show report         # details and upcoming run times
koyomi run report          # run now on its host, stream the output (Ctrl-C stops the run)
koyomi stop report         # stop the active run
koyomi logs report         # output of the latest run, each line with its time
koyomi history --failed    # past runs
koyomi status              # hosts, scheduler health, failing jobs, email alert status
koyomi tui                 # live terminal dashboard
koyomi fix                 # a coding agent in tmux fixes the failing jobs
koyomi disable report      # or: enable, update, delete
```

A job's `--host` defaults to this machine, and `--cwd` is required for a job on another host. `--cron` and `--at` use the job's `--tz` (default: this machine's zone). A job never runs twice for the same slot and never overlaps itself. If a slot was missed (the host was down, asleep, or could not reach the hub), it runs once when the host is back; `--catchup skip` skips it instead.

## Fixing failed jobs

Add [the Koyomi skill](skill/koyomi/) to your coding agent's skills directory before running `koyomi fix`. The skill reads private setup details from `site-notes.md` in your Koyomi state directory when present.

`koyomi fix` starts a coding agent's interactive UI, in full-auto mode, in a new tmux pane (a new tmux session if you are not in one), with a prompt that sends it to the koyomi skill's "Fixing failed jobs" section. For one job it works in that job's directory; for several it is an orchestrator that triages the jobs, then asks you to pick a plan: fix them all itself, or hand some to helper agents, each in its own pane, by running `koyomi fix <job>` itself.

The orchestrator runs at low effort, on a model hard-coded in `ORCHESTRATOR_FLAGS` in `koyomi.py`:

| Agent | Orchestrator flags |
| --- | --- |
| `claude` | `--model claude-opus-5-5 --effort low` |
| `codex` | `--model gpt-6.1-sol -c model_reasoning_effort="low"` |
| `pi`, `opencode` | none (their defaults) |

A fixer for one job, including a helper, uses the agent's own defaults.

```bash
koyomi fix                          # every failing job
koyomi fix report -a codex -m "the API key was rotated yesterday"
```

`--agent` is `claude`, `codex`, `pi` or `opencode`; the default is the first of those installed. The command is typed into the pane's shell, so your shell aliases and functions apply. The agent finds the cause, fixes it, reruns the job when that is safe, and leaves its changes uncommitted.

## Alerts

Email alerts are optional. `koyomi init` prints when SMTP is disabled, and `koyomi status` and `koyomi list` show where it is disabled. The list warning goes to stderr so `koyomi list --json` can still be piped to other tools. With hub SMTP disabled, Koyomi saves run results and logs and skips queueing new email alerts.

To enable email, include all three options in the host's init command. Use an SMTP server with STARTTLS and a private dotenv file with `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER` and `SMTP_PASSWORD`.

```sh
koyomi init server --hub-local --always-on --smtp-env /path/to/smtp.env \
  --email-from "Koyomi <koyomi@example.com>" --email-to operator@example.com
```

For an already registered host, repeat its original hub and host options when running `init`, then run `koyomi service restart` to load the new settings. Configure SMTP on the hub for scheduler alerts and on each additional host that should email when it loses contact with the hub.

The hub emails an alert when:

- a run fails, times out, or is interrupted (its runner died: reboot, crash, kill)
- a slot is skipped because the previous run was still going
- a job on an always-on host runs a slot late, or skips it with `--catchup skip`
- another always-on host stops reporting for 3 minutes

A job alerts once per failure streak: after a failed, timed-out, interrupted or skipped run it stays quiet until a run succeeds. A host with its own SMTP configuration emails when it loses contact with the hub for 3 minutes. Stopping a run with `koyomi stop` is not an alert. A failed send is retried after a minute, then with the wait doubling up to an hour; unsent alerts are shown by `koyomi status`.

## When something is down

- **The hub:** no run starts anywhere. Runs in progress continue; their output is spooled on the host and uploaded when the hub is back, and the runner waits to report its result. Missed slots then run once.
- **A host:** its jobs don't run. When it is back, missed slots run once, and runs it lost are marked `interrupted`.

## Terminal dashboard

`koyomi tui` shows every host's status, every job's state, next run, and last outcome. It reloads from the hub every 2 seconds (`--refresh SECONDS`).

Recent runs shows one segment per recorded run, oldest to newest, for each job
(up to 50 retained records). `h`/`l` or `←`/`→` select an older/newer segment;
the details below show its status, historical machine, trigger, duration, exit
code, local timestamps and error or scheduled slot. `Enter` opens that run on
the job page. Returning to the newest segment follows new runs as they arrive.
Scheduling columns appear as terminal width allows; the job page has full facts.

Symbols accompany color: `│` success, `!` failed/timed out/interrupted,
`▶` running, `○` queued, `/` explicitly skipped, `■` stopped, and `▾` selected.
Time without a recorded run has no segment. Slots missed during sleep do not
become failures; catch-up work appears as its actual run.

The compact chart shows actual completions per hour over the preceding 24 hours,
using retained history across all jobs. Skips and cancellations before execution
are excluded. Frequent jobs can have less than 24 hours of retained records, so
this is an activity view of available history. It refreshes with the snapshot;
it does not extrapolate throughput. Short terminals show the completion count.
`koyomi tui --reduced-motion` keeps activity indicators static while data refreshes.

| key | action |
| --- | --- |
| `j`/`k`, `↑`/`↓`, `g`, PgUp/PgDn | move the selection |
| `Enter` | open the job page |
| `h`/`l`, `←`/`→` | select an older / newer recent run |
| `e` | enable / disable (an active run keeps going) |
| `r` | run now on the job's host (asks first) |
| `x` | stop the active run (asks first) |
| `d` | delete the job and its history (asks first) |
| `/` | search id, host, description and command; `n` for the next match; `Esc` clears |
| `s` | sort by id, host, next run, or state |
| `?` | all keys |
| `q` | close what is open, or quit |

The job page shows the job's command, schedule and options, its runs (newest first) and the tail of the picked run's log, live while it runs. `j`/`k` pick a run, `Enter` downloads its whole log and opens it, `e`/`r`/`x` work as on the list, and `Esc` goes back.

Inside a log: `j`/`k` scroll, `ctrl+d`/`ctrl+u` page, `g` jumps to the beginning, `f` toggles live follow, and `/` searches. Press `n` for the next match.

Every output line is stamped with the time it was written; the dashboard and `koyomi logs` show it in local time. Logs from before stamping show without times.

Creating and editing jobs stays in the CLI.

## Scheduler

```bash
koyomi service status
koyomi service restart
```

The service also supports `start`, `stop`, and `uninstall`. Scheduler process logs (hub unreachable, errors) are in `~/.koyomi/launchd.log` on macOS and `journalctl -u koyomi` on Linux. Scheduling events from every host are in the hub's `daemon.log` (`koyomi logs --daemon`).

## Data

On the hub:

```
~/.koyomi/
├── config.json               # this machine (mode 600)
├── jobs/<id>.json            # job definition and state
├── runs/<id>/<run_id>.json   # run record: host, status, exit code, timing
├── runs/<id>/<run_id>.log    # run output
├── hosts/<name>.json         # registered hosts and their heartbeat
├── alerts/<alert_id>.json    # alert emails, kept after sending
└── daemon.log                # scheduling events from every host
```

Other hosts keep `config.json`, `daemon.json` (local scheduler heartbeat) and `spool/` (output of runs in progress).

## License

Koyomi is released under [GPL-3.0-only](LICENSE).

<p align="center">
  <a href="https://zazencodes.com/?utm_source=github&utm_medium=referral&utm_campaign=koyomi">
    <img src="docs/assets/zazencodes-banner.png" alt="Made by ZazenCodes" width="600">
  </a>
</p>
