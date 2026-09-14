#!/bin/sh
# Install (or reinstall) Koyomi: CLI via uv tool, launchd agent, global agent skill.
set -eu
cd "$(dirname "$0")"
REPO="$(pwd)"

uv tool install --reinstall --quiet "$REPO"
KOYOMI="$(uv tool dir --bin)/koyomi"

# launchd agent (captures the current PATH for scheduled jobs)
"$KOYOMI" service install

# global coding-agent skill
for dir in "$HOME/.agents/skills" "$HOME/.claude/skills"; do
  mkdir -p "$dir"
  ln -sfn "$REPO/skill/koyomi" "$dir/koyomi"
done

echo "koyomi installed: $KOYOMI"
"$KOYOMI" status
