#!/bin/sh
# Install (or reinstall) Koyomi on this machine: CLI via uv tool, scheduler service
# (launchd agent on macOS, systemd unit on Linux), global agent skill.
# A new machine runs `koyomi init` first (see README.md).
set -eu
cd "$(dirname "$0")"
REPO="$(pwd)"

uv tool install --reinstall --quiet "$REPO"
KOYOMI="$(uv tool dir --bin)/koyomi"

# global coding-agent skill
for dir in "$HOME/.agents/skills" "$HOME/.claude/skills"; do
  mkdir -p "$dir"
  ln -sfn "$REPO/skill/koyomi" "$dir/koyomi"
done

# scheduler service (captures the current PATH for scheduled jobs)
"$KOYOMI" service install

echo "koyomi installed: $KOYOMI"
"$KOYOMI" status
