# Koyomi

A small job scheduler for a few machines. One Python file (stdlib only), plain JSON state, a launchd agent on macOS and a systemd service on Linux.

One machine is the **hub**: it holds every job, run record and log in `~/.koyomi/`. Every job is pinned to a **host** that runs it. Each host runs a scheduler that asks the hub for its due runs, runs them, and streams their output back. Other machines reach the hub with `ssh <hub> koyomi _rpc`, so the CLI and the dashboard show and control every job on every host from any machine.

Hosts are either **always on** (a server: runs on schedule, and a late or missed slot raises an alert) or **when awake** (a laptop: runs while awake and online; a slot missed while asleep runs once on wake).

## Install

On every machine, from a checkout of this repo:

```bash
uv tool install .                     # the koyomi CLI
koyomi init HOST ...                  # once: name this host, point it at the hub (below)
./install.sh                          # CLI + scheduler service + agent skill; re-run after editing the code
```

`koyomi init` stores `~/.koyomi/config.json` (mode 600): the host name, how to reach the hub, and the SMTP settings for alert email (read from a dotenv file with `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`).

```bash
# the hub, an always-on server
koyomi init emanator --hub-local --always-on --smtp-env smtp.env \
  --email-from "Koyomi <koyomi@mail.zazencodes.com>" --email-to you@example.com

# a laptop that reaches the hub over ssh (key-based login, no passphrase prompt)
koyomi init mac --hub-ssh emanator --hub-koyomi /root/.local/bin/koyomi --smtp-env smtp.env \
  --email-from "Koyomi <koyomi@mail.zazencodes.com>" --email-to you@example.com
```

`./install.sh` installs the service (launchd agent on macOS; systemd unit on Linux, as root), which bakes in the current shell's `PATH` for scheduled jobs. Hub and hosts must run the same Koyomi version: a mismatch fails every call until both are reinstalled.

## Usage

```bash
koyomi add backup --cron "30 2 * * *" --cwd ~/notes --cmd "git commit -qam backup || true"
koyomi add report --host emanator --tz America/Toronto --cron "0 6 * * *" \
  --cwd /root/app --timeout 3h -- bin/report daily
koyomi add remind --in 2h --cmd "say stretch"

koyomi list                # jobs, host, next run, last result
koyomi show report         # details and upcoming run times
koyomi run report          # run now on its host, stream the output (Ctrl-C stops the run)
koyomi stop report         # stop the active run
koyomi logs report         # output of the latest run
koyomi history --failed    # past runs
koyomi status              # hosts, scheduler health, failing jobs, unsent alerts
koyomi tui                 # live terminal dashboard
koyomi disable report      # or: enable, update, delete
```

A job's `--host` defaults to this machine, and `--cwd` is required for a job on another host. `--cron` and `--at` use the job's `--tz` (default: this machine's zone). A job never runs twice for the same slot and never overlaps itself. If a slot was missed (the host was down, asleep, or could not reach the hub), it runs once when the host is back; `--catchup skip` skips it instead.

## Alerts

The hub emails an alert when:

- a run fails, times out, or is interrupted (its runner died: reboot, crash, kill)
- a slot is skipped because the previous run was still going
- a job on an always-on host runs a slot late, or skips it with `--catchup skip`
- another always-on host stops reporting for 3 minutes

A job alerts once per failure streak: after a failed, timed-out, interrupted or skipped run it stays quiet until a run succeeds. A machine that cannot reach the hub for 3 minutes emails that itself. Stopping a run with `koyomi stop` is not an alert. A failed send is retried after a minute, then with the wait doubling up to an hour; unsent alerts are shown by `koyomi status`.

## When something is down

- **The hub:** no run starts anywhere. Runs in progress continue; their output is spooled on the host and uploaded when the hub is back, and the runner waits to report its result. Missed slots then run once.
- **A host:** its jobs don't run. When it is back, missed slots run once, and runs it lost are marked `interrupted`.

## Terminal dashboard

`koyomi tui` shows every host's status, every job's state, next run, and last outcome. It reloads from the hub every 2 seconds (`--refresh SECONDS`).

| key | action |
| --- | --- |
| `j`/`k`, `↑`/`↓`, `g`/`G`, PgUp/PgDn | move the selection |
| `Enter` | toggle the detail pane (command, host, cwd, options, last run) |
| `e` | enable / disable (an active run keeps going) |
| `r` | run now on the job's host |
| `x` | stop the active run (asks first) |
| `d` | delete the job and its history (asks first) |
| `l` / `h` | log of the latest run (live-follows) / run history |
| `D` | scheduler log of every host |
| `/` | filter on id, host, description or command; `Esc` clears |
| `s` | sort by id, host, next run, or state |
| `?` | all keys |
| `q` | close what is open, or quit |

Inside a log or history view: `j`/`k` scroll, `Ctrl-D`/`Ctrl-U` page, `g`/`G` jump, `f` toggles live follow.

Creating and editing jobs stays in the CLI.

## Scheduler

```bash
koyomi service status | start | stop | restart | uninstall
```

Scheduler process logs (hub unreachable, errors) are in `~/.koyomi/launchd.log` on macOS and `journalctl -u koyomi` on Linux. Scheduling events from every host are in the hub's `daemon.log` (`koyomi logs --daemon`).

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
