#!/usr/bin/env bash
# Run a command inside the sfnkit dev sandbox (see isb.yaml) with mise tools on PATH.
# Usage: scripts/sandbox.sh [-e NAME ...] -- <shell command>
set -euo pipefail
cd "$(dirname "$0")/.."
envs=()
while [[ $# -gt 0 && $1 != "--" ]]; do envs+=("$1"); shift; done
[[ ${1:-} == "--" ]] && shift
exec isb exec "${envs[@]}" dev -- /bin/bash -c "export PATH=\$HOME/.local/share/mise/shims:\$HOME/.local/bin:\$PATH; cd /home/dev/sfnkit; $*"
