#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_dev.sh
# Run the C360 backend (FastAPI/uvicorn) and frontend (Vite) together with one
# command. Validates the configuration up front and prints which database the
# backend will connect to, so you never start against the wrong one.
#
# Usage:
#   ./scripts/run_dev.sh                 # honor exported DATABASE_URL, else local
#   ./scripts/run_dev.sh --db local      # force the local PostgreSQL DSN
#   ./scripts/run_dev.sh --db rds        # AWS RDS (DATABASE_URL must be set)
#   ./scripts/run_dev.sh --db-url URL    # explicit DATABASE_URL (any target)
#
# Database selection (first match wins):
#   1. --db-url URL        use URL verbatim
#   2. --db rds            use $DATABASE_URL (error if unset)
#   3. --db local          use the local container DSN, ignoring $DATABASE_URL
#   4. no flag + $DATABASE_URL set   → honor it (e.g. from set_env_from_tf.sh)
#   5. no flag + unset     → local container DSN (default)
#
# To load RDS settings from Terraform first, SOURCE the helper so the exports
# reach this script (executing it in a subshell loses them):
#   source scripts/set_env_from_tf.sh && ./scripts/run_dev.sh
#
# Options:
#   --db local|rds   Force a target. Omit to auto-detect from $DATABASE_URL.
#   --db-url URL     Override DATABASE_URL explicitly (implies a custom target).
#   --no-frontend    Start the backend only.
#   --no-backend     Start the frontend only.
#   --check          Validate + print config, then exit without starting.
#   -h, --help       Show this help.
#
# Environment overrides:
#   DATABASE_URL     Required for --db rds (a postgresql://… DSN). Read from the
#                    environment so secrets never land in a file (per repo policy).
#   SINK             Backend sink. Default: postgres.
#   CDC_CONNECTOR_ENABLED
#                    true  ⇒ a CDC connector owns Kafka, so the backend never
#                    emits (prevents double-publishing cdc.public.*). Forced true
#                    by --db rds, false by --db local; otherwise honored if set,
#                    else inferred from the host (only localhost ⇒ false; any
#                    remote host ⇒ true, the safe no-double-publish default).
#   BACKEND_PORT     Default: 8000
#   FRONTEND_PORT    Default: 5173
#   LOCAL_PG_*       LOCAL_PG_USER / LOCAL_PG_PASSWORD / LOCAL_PG_HOST /
#                    LOCAL_PG_PORT / LOCAL_PG_DB override the local DSN parts.
#
# Exported env vars take precedence over apps/backend/.env (pydantic-settings
# reads real environment variables before the dotenv file), so this script's
# DATABASE_URL / SINK always win — whatever .env happens to contain.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKEND_DIR="${REPO_ROOT}/apps/backend"
FRONTEND_DIR="${REPO_ROOT}/apps/frontend"

# ── Helpers ─────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }
warn() { echo "[$(date '+%H:%M:%S')] WARN:  $*"; }
die()  { err "$*"; exit 1; }

# Replace the password in a postgresql://user:pass@host DSN with ****.
mask_dsn() {
  echo "$1" | sed -E 's#(://[^:/@]+:)[^@]*@#\1****@#'
}

# Extract host:port/db from a DSN for a friendly one-line summary.
dsn_target() {
  echo "$1" | sed -E 's#^[^@]*@##; s#\?.*$##'
}

# Print the leading comment block (everything after the shebang up to the first
# non-comment, non-blank line) as help — robust to edits that shift line numbers.
usage() {
  awk 'NR==1 && /^#!/ {next} /^#/ {sub(/^# ?/,""); print; next} /^$/ {print; next} {exit}' "${BASH_SOURCE[0]}"
}

# ── Argument parsing ────────────────────────────────────────────────────────
DB_TARGET=""               # "" => auto-detect | local | rds | custom | env
DB_URL_OVERRIDE=""
RUN_BACKEND=true
RUN_FRONTEND=true
CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)          DB_TARGET="${2:-}"; shift 2 ;;
    --db=*)        DB_TARGET="${1#*=}"; shift ;;
    --db-url)      DB_URL_OVERRIDE="${2:-}"; DB_TARGET="custom"; shift 2 ;;
    --db-url=*)    DB_URL_OVERRIDE="${1#*=}"; DB_TARGET="custom"; shift ;;
    --no-frontend) RUN_FRONTEND=false; shift ;;
    --no-backend)  RUN_BACKEND=false; shift ;;
    --check)       CHECK_ONLY=true; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             die "Unknown option: $1 (use --help)" ;;
  esac
done

case "$DB_TARGET" in
  ""|local|rds|custom) ;;
  *) die "--db must be 'local' or 'rds' (got '$DB_TARGET')" ;;
esac

# No explicit flag: honor an already-exported DATABASE_URL, else fall back local.
if [[ -z "$DB_TARGET" ]]; then
  if [[ -n "${DATABASE_URL:-}" ]]; then
    DB_TARGET="env"
  else
    DB_TARGET="local"
  fi
fi

$RUN_BACKEND || $RUN_FRONTEND || die "Nothing to run: --no-backend and --no-frontend are mutually exclusive."

# ── Resolve configuration ───────────────────────────────────────────────────
SINK="${SINK:-postgres}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"

LOCAL_PG_USER="${LOCAL_PG_USER:-dbadmin}"
LOCAL_PG_PASSWORD="${LOCAL_PG_PASSWORD:-localdevonly}"
LOCAL_PG_HOST="${LOCAL_PG_HOST:-localhost}"
LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
LOCAL_PG_DB="${LOCAL_PG_DB:-c360db}"
LOCAL_DSN="postgresql://${LOCAL_PG_USER}:${LOCAL_PG_PASSWORD}@${LOCAL_PG_HOST}:${LOCAL_PG_PORT}/${LOCAL_PG_DB}"

DB_LABEL=""
case "$DB_TARGET" in
  local)
    RESOLVED_DSN="$LOCAL_DSN"
    DB_LABEL="Local PostgreSQL (container from apps/backend/start_local_pg_server.sh)"
    ;;
  rds)
    RESOLVED_DSN="${DATABASE_URL:-}"
    [[ -n "$RESOLVED_DSN" ]] || die "--db rds requires DATABASE_URL to be exported (never hard-coded here).
         Load it from Terraform first:  source scripts/set_env_from_tf.sh
         or export manually:  export DATABASE_URL='postgresql://dbadmin:<pwd>@<rds-endpoint>:5432/c360db'"
    DB_LABEL="AWS RDS PostgreSQL"
    ;;
  env)
    RESOLVED_DSN="$DATABASE_URL"
    case "$(dsn_target "$RESOLVED_DSN")" in
      *rds.amazonaws.com*) DB_LABEL="AWS RDS PostgreSQL (from \$DATABASE_URL)" ;;
      localhost*|127.0.0.1*) DB_LABEL="Local PostgreSQL (from \$DATABASE_URL)" ;;
      *)                   DB_LABEL="From environment \$DATABASE_URL" ;;
    esac
    ;;
  custom)
    RESOLVED_DSN="$DB_URL_OVERRIDE"
    DB_LABEL="Custom target (--db-url)"
    ;;
esac

# CDC ownership of Kafka. A managed Debezium connector publishes cdc.public.*
# for RDS / remote PostgreSQL, so the backend must NOT dual-write there.
#   --db rds   → always force true (connector owns Kafka), overriding any
#                exported value, so RDS can never double-publish.
#   --db local → always false.
#   otherwise  → honor an exported CDC_CONNECTOR_ENABLED (e.g. from
#                set_env_from_tf.sh), else infer from the host.
if [[ "$DB_TARGET" == "rds" ]]; then
  CDC_CONNECTOR_ENABLED=true
elif [[ "$DB_TARGET" == "local" ]]; then
  CDC_CONNECTOR_ENABLED=false
elif [[ -z "${CDC_CONNECTOR_ENABLED:-}" ]]; then
  # Safe default: only an explicitly local host is treated as "app may emit".
  # Anything else (RDS or any remote PG) is assumed connector-owned, since a
  # wrong "false" double-publishes whereas a wrong "true" merely goes quiet.
  case "$(dsn_target "$RESOLVED_DSN")" in
    localhost*|127.0.0.1*) CDC_CONNECTOR_ENABLED=false ;;
    *)                     CDC_CONNECTOR_ENABLED=true ;;
  esac
fi

# ── Validation ──────────────────────────────────────────────────────────────
log "Validating configuration…"

if [[ "$SINK" != "postgres" ]]; then
  warn "SINK=$SINK — this script targets the PostgreSQL sink; DB settings below are ignored by the backend."
fi

[[ "$RESOLVED_DSN" == postgresql://* || "$RESOLVED_DSN" == postgres://* ]] \
  || die "DATABASE_URL does not look like a postgresql:// DSN: $(mask_dsn "$RESOLVED_DSN")"

if $RUN_BACKEND; then
  [[ -d "$BACKEND_DIR" ]]       || die "Backend directory not found: $BACKEND_DIR"
  command -v uv >/dev/null 2>&1 || die "'uv' not found on PATH — install it (https://docs.astral.sh/uv/) to run the backend."
fi

if $RUN_FRONTEND; then
  [[ -d "$FRONTEND_DIR" ]]       || die "Frontend directory not found: $FRONTEND_DIR"
  command -v npm >/dev/null 2>&1 || die "'npm' not found on PATH — install Node.js to run the frontend."
fi

# Reachability check for the local DB (best effort; warn only).
if $RUN_BACKEND && [[ "$SINK" == "postgres" && "$DB_TARGET" == "local" ]]; then
  if command -v nc >/dev/null 2>&1 && ! nc -z "$LOCAL_PG_HOST" "$LOCAL_PG_PORT" >/dev/null 2>&1; then
    warn "No PostgreSQL listening on ${LOCAL_PG_HOST}:${LOCAL_PG_PORT}."
    warn "Start it first:  (cd apps/backend && ./start_local_pg_server.sh)"
  fi
fi

VITE_API_BASE_URL="${VITE_API_BASE_URL:-http://localhost:${BACKEND_PORT}/api/v1}"

# ── Summary ─────────────────────────────────────────────────────────────────
echo
echo "─────────────────────────────────────────────────────────────"
echo "  C360 dev stack"
echo "─────────────────────────────────────────────────────────────"
printf '  %-14s %s\n' "DATABASE:" "$DB_LABEL"
printf '  %-14s %s\n' "DATABASE_URL:" "$(mask_dsn "$RESOLVED_DSN")"
printf '  %-14s %s\n' "→ target:" "$(dsn_target "$RESOLVED_DSN")"
printf '  %-14s %s\n' "SINK:" "$SINK"
printf '  %-14s %s\n' "CDC conn.:" "$CDC_CONNECTOR_ENABLED  (true ⇒ app does NOT emit Kafka; connector owns cdc.public.*)"
$RUN_BACKEND  && printf '  %-14s %s\n' "BACKEND:"  "http://localhost:${BACKEND_PORT}  (docs: /docs)"
$RUN_FRONTEND && printf '  %-14s %s\n' "FRONTEND:" "http://localhost:${FRONTEND_PORT}"
$RUN_FRONTEND && printf '  %-14s %s\n' "API base:" "$VITE_API_BASE_URL"
echo "─────────────────────────────────────────────────────────────"
echo

if $CHECK_ONLY; then
  log "Configuration OK (--check): nothing started."
  exit 0
fi

# ── Launch ──────────────────────────────────────────────────────────────────
export SINK
export DATABASE_URL="$RESOLVED_DSN"
export CDC_CONNECTOR_ENABLED
export VITE_API_BASE_URL

BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
  trap - INT TERM EXIT
  echo
  log "Shutting down…"
  [[ -n "$FRONTEND_PID" ]] && kill "$FRONTEND_PID" 2>/dev/null || true
  if [[ -n "$BACKEND_PID" ]]; then
    pkill -P "$BACKEND_PID" 2>/dev/null || true   # uvicorn --reload child
    kill "$BACKEND_PID" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

if $RUN_BACKEND; then
  log "Starting backend on :${BACKEND_PORT} …"
  ( cd "$BACKEND_DIR" && exec uv run uvicorn main:app --reload --host 0.0.0.0 --port "$BACKEND_PORT" ) &
  BACKEND_PID=$!
fi

if $RUN_FRONTEND; then
  if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
    log "frontend/node_modules missing — running 'npm install' …"
    ( cd "$FRONTEND_DIR" && npm install )
  fi
  log "Starting frontend on :${FRONTEND_PORT} …"
  ( cd "$FRONTEND_DIR" && exec npm run dev -- --port "$FRONTEND_PORT" ) &
  FRONTEND_PID=$!
fi

log "Both services launched. Press Ctrl-C to stop."

# Bash 3.2 has no 'wait -n'; poll until either child exits, then tear down.
while :; do
  if [[ -n "$BACKEND_PID" ]] && ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    err "Backend exited."; break
  fi
  if [[ -n "$FRONTEND_PID" ]] && ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
    err "Frontend exited."; break
  fi
  sleep 1
done
