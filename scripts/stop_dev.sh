#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# stop_dev.sh
# Stop the C360 backend (FastAPI/uvicorn) and frontend (Vite) started by
# run_dev.sh. run_dev.sh normally tears both down on Ctrl-C, so use this when it
# was backgrounded, the terminal was closed, or a hard kill left stragglers
# holding the ports. Processes are located by the port they listen on, so no PID
# file is needed and it works regardless of how they were launched.
#
# Usage:
#   ./scripts/stop_dev.sh                 # stop backend + frontend
#   ./scripts/stop_dev.sh --no-frontend   # stop the backend only
#   ./scripts/stop_dev.sh --no-backend    # stop the frontend only
#   ./scripts/stop_dev.sh --force         # SIGKILL immediately (skip graceful)
#
# Options:
#   --no-frontend    Leave the frontend running.
#   --no-backend     Leave the backend running.
#   --force          Send SIGKILL right away instead of SIGTERM-then-SIGKILL.
#   -h, --help       Show this help.
#
# Environment overrides:
#   BACKEND_PORT     Default: 8000  (must match the run_dev.sh value).
#   FRONTEND_PORT    Default: 5173  (must match the run_dev.sh value).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Helpers ─────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }
warn() { echo "[$(date '+%H:%M:%S')] WARN:  $*"; }
die()  { err "$*"; exit 1; }

# Print the leading comment block (everything after the shebang up to the first
# non-comment, non-blank line) as help — robust to edits that shift line numbers.
usage() {
  awk 'NR==1 && /^#!/ {next} /^#/ {sub(/^# ?/,""); print; next} /^$/ {print; next} {exit}' "${BASH_SOURCE[0]}"
}

# PIDs listening on a TCP port. lsof is on macOS by default; fall back to fuser.
pids_on_port() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti "tcp:${port}" -s TCP:LISTEN 2>/dev/null || true
  elif command -v fuser >/dev/null 2>&1; then
    fuser "${port}/tcp" 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' || true
  else
    die "Neither 'lsof' nor 'fuser' found on PATH — cannot locate processes by port."
  fi
}

# Stop everything listening on a port. Graceful (SIGTERM, wait, then SIGKILL)
# unless --force. Returns success whether or not anything was running.
stop_port() {
  local label="$1" port="$2"
  local pids
  pids="$(pids_on_port "$port")"

  if [[ -z "$pids" ]]; then
    log "${label}: nothing listening on :${port}."
    return 0
  fi

  # shellcheck disable=SC2086  # intentional word-splitting of the PID list
  log "${label}: stopping PID(s) $(echo $pids | tr '\n' ' ')on :${port} …"

  if $FORCE; then
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
  else
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    # Give them up to ~5s to exit cleanly, then force any survivors.
    for _ in 1 2 3 4 5; do
      pids="$(pids_on_port "$port")"
      [[ -z "$pids" ]] && break
      sleep 1
    done
    pids="$(pids_on_port "$port")"
    if [[ -n "$pids" ]]; then
      # shellcheck disable=SC2086
      warn "${label}: still up after SIGTERM — sending SIGKILL to $(echo $pids | tr '\n' ' ')."
      # shellcheck disable=SC2086
      kill -9 $pids 2>/dev/null || true
    fi
  fi

  if [[ -n "$(pids_on_port "$port")" ]]; then
    err "${label}: could not free :${port} (check permissions / 'lsof -i :${port}')."
    return 1
  fi
  log "${label}: stopped."
}

# ── Argument parsing ────────────────────────────────────────────────────────
STOP_BACKEND=true
STOP_FRONTEND=true
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-frontend) STOP_FRONTEND=false; shift ;;
    --no-backend)  STOP_BACKEND=false; shift ;;
    --force)       FORCE=true; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             die "Unknown option: $1 (use --help)" ;;
  esac
done

$STOP_BACKEND || $STOP_FRONTEND || die "Nothing to stop: --no-backend and --no-frontend are mutually exclusive."

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

# ── Stop ────────────────────────────────────────────────────────────────────
rc=0
$STOP_BACKEND  && { stop_port "BACKEND"  "$BACKEND_PORT"  || rc=1; }
$STOP_FRONTEND && { stop_port "FRONTEND" "$FRONTEND_PORT" || rc=1; }

exit $rc
