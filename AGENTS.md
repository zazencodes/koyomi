# AGENTS.md

## Project Instructions

- Keep Koyomi small: stdlib-only Python, plain JSON state, no database or extra services. Don't add dependencies without asking.
- macOS (launchd) and Linux (systemd) only.

## Repo Shape

- `koyomi.py`: the whole thing. Sections, top to bottom: cron parser; hub state and operations (`op_*`, registered in `HUB_OPS`), which run only on the hub; hub access (`hub()` / `call_hub()`, the `_rpc` server); email alerts; the runner (`execute`, hidden `_exec` subcommand); the host daemon (`host_tick`, `daemon_main`); launchd/systemd management (`service`); CLI commands; TUI.
- `skill/koyomi/SKILL.md`: global agent skill. It is an external skill of the `~/agents` registry (`EXTERNAL_SKILLS` in `~/agents/skills/sync_skills.py`), which symlinks this folder into `~/.agents/skills`, `~/.claude/skills` and Gemini's skills, so edits go live immediately.
- `install.sh`: `uv tool install --reinstall .` + `koyomi service install`.
- `tests/test_koyomi.py`: unittest suite.

## Architecture

- The hub (here: `emanator`, `/root/koyomi`) owns all state. On the hub, `hub()` calls `op_*` in-process; elsewhere it runs `config.json`'s `hub.command` (`ssh … emanator /root/.local/bin/koyomi _rpc`) with a JSON request on stdin. Both sides check `VERSION`, so bump it when an op's arguments or results change, and reinstall every machine.
- Hosts hold no job state: `config.json`, `daemon.json`, `spool/<run_id>.log` (output not yet uploaded).

## Workflow

- The installed CLI/daemon is a **copy** in the uv tool venv, not this checkout. After changing `koyomi.py`, run `./install.sh` on every machine (on the hub first): `rsync`/`git pull` the checkout to `emanator:/root/koyomi`, then `ssh emanator 'cd /root/koyomi && ./install.sh'`, then `./install.sh` here.
- After every change to `koyomi.py`, run `./install.sh` on this Mac without being asked, so `koyomi` (including `koyomi tui`) runs the new code. A change that touches the hub (ops, `VERSION`, daemon) still goes to the hub first, as above.
- Set `KOYOMI_HOME=/some/tmp/dir` with a `config.json` whose `hub` is `"local"` to experiment without touching real state.
- `service install` bakes the current shell's `PATH` (and `HOME`) into the plist/unit; scheduled jobs use that PATH.

## Important Notes

- Duplicate protection depends on ordering in `op_tick()`: persist the advanced `next_run` + `running` claim **before** the host spawns the runner. Keep hub state mutations inside `store_lock()` and writes through `write_json()` (atomic).
- The daemon reaps runners by polling them; don't set `SIGCHLD` to `SIG_IGN` (it would make every `subprocess.run`, including ssh to the hub, report exit 0).
- Dead runners are found by their host (pid liveness + boot time, `runner_gone`) and reported with `mark_interrupted`.
- `run_progress` appends at a byte offset and is idempotent, so runners can retry through hub outages.
- The runner pipes command output through `StampedLog`, which starts every line with its UTC time (`LOG_STAMP`); `split_log_line` renders it. The TUI makes every hub call on its `HubWorker` thread, never on the UI thread.
- The systemd unit uses `KillMode=process` so restarting Koyomi doesn't kill runs in flight. launchd `bootout` is async; `service install` waits before `bootstrap`.

## Validation

- `python3 -m unittest discover -s tests -v` (~8s; spawns real short-lived processes, including `_rpc` servers).
- For scheduler changes, also check live: reinstall hub and Mac, add a `--cron "* * * * *"` job on each host, confirm with `koyomi history` / `koyomi logs --daemon`, then delete them.
