#!/bin/sh
# Install (or reinstall) Koyomi on this machine: CLI via uv tool and scheduler service
# (launchd agent on macOS, systemd unit on Linux). A new machine runs `koyomi init` first
# (see README.md). The agent skill is linked by the ~/agents skills registry, not here.
set -eu
cd "$(dirname "$0")"

uv tool install --reinstall --quiet .
KOYOMI="$(uv tool dir --bin)/koyomi"

# scheduler service (captures the current PATH for scheduled jobs)
"$KOYOMI" service install

echo "koyomi installed: $KOYOMI"
"$KOYOMI" status
