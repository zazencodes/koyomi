# AGENTS.md

## Project Instructions

- Keep Koyomi small: stdlib-only Python, plain JSON state, no database or extra services. Don't add dependencies without asking.
- macOS only (launchd, `sysctl kern.boottime`, `osascript`).

## Repo Shape

- `koyomi.py`: the whole thing. It holds the CLI (argparse), cron parser, scheduler `tick()`, run executor (`execute`, invoked as hidden `_exec` subcommand) and launchd management (`service`).
- `skill/koyomi/SKILL.md`: global agent skill. `~/.agents/skills/koyomi` and `~/.claude/skills/koyomi` are symlinks to this folder, so edits go live immediately.
- `install.sh`: `uv tool install --reinstall .` + `koyomi service install` + skill symlinks.
- `tests/test_koyomi.py`: unittest suite.

## Workflow

- The installed CLI/daemon is a **copy** in the uv tool venv, not this checkout. After changing `koyomi.py`, run `./install.sh`, which reinstalls and restarts the launchd agent.
- Set `KOYOMI_HOME=/some/tmp/dir` to experiment without touching the real `~/.koyomi/`.
- `service install` bakes the current shell's `PATH` into the plist; scheduled jobs use that PATH.

## Important Notes

- Duplicate protection depends on ordering in `tick()`: persist the advanced `next_run` + `running` claim **before** spawning the runner. Keep job-file mutations inside `store_lock()` and writes through `write_json()` (atomic).
- The daemon sets `SIGCHLD` to `SIG_IGN` to auto-reap runners, so don't `wait()` on subprocesses inside the daemon process.
- Stale `running` markers are cleared by pid liveness + boot time (`reconcile_running`).
- launchd `bootout` is async; `service install` waits before `bootstrap`.

## Validation

- `python3 -m unittest -v` (~5s; spawns real short-lived processes).
- For scheduler changes, also check live: `./install.sh`, add a `--cron "* * * * *"` job, confirm with `koyomi history` / `koyomi logs --daemon`, then delete it.
