# Koyomi

A tiny local job scheduler for macOS. One Python file (stdlib only), one launchd agent, plain JSON state in `~/.koyomi/`.

## Install

```bash
./install.sh
```

This installs the `koyomi` CLI with `uv tool`, registers and starts the launchd agent (it runs at login and restarts if it crashes), and symlinks the agent skill into `~/.agents/skills/koyomi` and `~/.claude/skills/koyomi`. Re-run it after editing the code.

## Usage

```bash
koyomi add backup --cron "30 2 * * *" --cwd ~/notes --cmd "git commit -qam backup || true"
koyomi add sync --every 15m --timeout 10m --notify --cmd ./sync.sh
koyomi add remind --in 2h --cmd "say stretch"

koyomi list                # jobs, next run, last result
koyomi show sync           # details and upcoming run times
koyomi run sync            # run now
koyomi stop sync           # interrupt the active run
koyomi logs sync           # output of the latest run
koyomi history --failed    # past runs
koyomi status              # scheduler health and failing jobs
koyomi tui                 # live terminal dashboard
koyomi disable sync        # or: enable, update, delete
```

If a job was due while the Mac was asleep or off, it runs once when Koyomi comes back. Use `--catchup skip` to skip it instead. A job never runs twice for the same slot, and never overlaps itself.

## Terminal dashboard

`koyomi tui` opens a live dashboard: scheduler health, every job's state, next run, and last outcome. It reads the same JSON files as the CLI and reloads once a second (`--refresh SECONDS`).

| key | action |
| --- | --- |
| `j`/`k`, `↑`/`↓`, `g`/`G`, PgUp/PgDn | move the selection |
| `Enter` | toggle the detail pane (command, cwd, options, last run) |
| `e` | enable / disable (an active run keeps going) |
| `r` | run now, in the background |
| `x` | stop the active run (asks first) |
| `d` | delete the job and its history (asks first) |
| `l` / `h` | log of the latest run (live-follows) / run history |
| `D` | scheduler log |
| `/` | filter on id, description or command; `Esc` clears |
| `s` | sort by id, next run, or state |
| `?` | all keys |
| `q` | close what is open, or quit |

Inside a log or history view: `j`/`k` scroll, `Ctrl-D`/`Ctrl-U` page, `g`/`G` jump, `f` toggles live follow.

Creating and editing jobs stays in the CLI — the dashboard only operates on jobs that already exist. Everything it can do to a job, the CLI can too (`run`, `stop`, `enable`/`disable`, `delete`, `logs`, `history`).

## Scheduler

```bash
koyomi service status | start | stop | restart | uninstall
```

## Data

```
~/.koyomi/
├── jobs/<id>.json            # job definition and state
├── runs/<id>/<run_id>.json   # run record: status, exit code, timing
├── runs/<id>/<run_id>.log    # run output
└── daemon.log                # scheduler events
```
