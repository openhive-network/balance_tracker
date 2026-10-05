#!/usr/bin/env bash
set -euo pipefail

schema=btracker_app
batch_size=10000
app_lock_name=
postgres_url=${POSTGRES_URL:-postgresql://haf_admin@localhost:5432/haf_block_log}

usage() {
  cat <<'HELP'
Usage: backfill_order_lifecycle.sh [OPTIONS]
Replay only lifecycle history after installing an upgrade on an existing context.
Stop the owning application processor first. The command holds its HAF install
lock, detaches the complete context group, and resumes the recorded checkpoint.

  --schema=NAME              Tracker context/schema (default btracker_app)
  --postgres-url=URL         Database connection (or POSTGRES_URL)
  --batch-size=N             Commit every N blocks (default 10000; max 100000)
  --app-lock-name=NAME       Override the owning driver's advisory lock name
  --help                    Show this help
HELP
}

for arg in "$@"; do
  case "$arg" in
    --schema=*) schema=${arg#*=} ;;
    --postgres-url=*) postgres_url=${arg#*=} ;;
    --batch-size=*) batch_size=${arg#*=} ;;
    --app-lock-name=*) app_lock_name=${arg#*=} ;;
    --help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
if [[ ! $schema =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] || [[ ! $batch_size =~ ^[0-9]+$ ]]; then
  echo 'Invalid schema or batch-size' >&2
  exit 2
fi

# A single dedicated psql session retains the install lock across procedure
# commits and releases it on success, error, interruption, or connection loss.
psql "$postgres_url" -X -v ON_ERROR_STOP=1 \
  -v schema="$schema" -v batch_size="$batch_size" -v app_lock_name="$app_lock_name" <<'SQL'
SET ROLE btracker_owner;
SET search_path TO :"schema";
CALL backfill_order_lifecycle(:'schema', :batch_size, NULLIF(:'app_lock_name', ''));
SQL
