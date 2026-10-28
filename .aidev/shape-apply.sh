#!/usr/bin/env bash
# performance.shape.apply (.aidev/project.yaml): installs this tree's query code
# (types, backend helpers, endpoints) on a copy of the db-shape, in the order
# scripts/install_app.sh installs it, as the block explorer's embedded instance
# (schema hafbe_bal) that the shape was captured from.
#
#   AIDEV_DB_SHAPE_PSQL=<psql invocation reading SQL on stdin> .aidev/shape-apply.sh
#
# A shape holds no rows, only the source's schema and statistics, so the files
# that create the application's tables and HAF context (db/btracker_app.sql,
# db/builtin_roles.sql) and the block-processing code (db/process_*.sql) are not
# run: they would replace the shape's tables, and with them their statistics,
# and nothing an API query plans with lives in them. Every other file
# install_app.sh feeds to psql is run, in its order, through
# $AIDEV_DB_SHAPE_PSQL on stdin (the shape server has no network and cannot
# read a -f path from this tree).
set -euo pipefail
cd "$(dirname "$0")/.."

: "${AIDEV_DB_SHAPE_PSQL:?set by aidev db shape: the psql invocation reaching the shape copy}"
: "${AIDEV_DB_SHAPE_DATABASE:?set by aidev db shape: the shape copy psql reaches}"
schema="${BTRACKER_SCHEMA:-hafbe_bal}"

shape_psql=()
eval "shape_psql=($AIDEV_DB_SHAPE_PSQL)"

# A shape restores the source's btracker schemas owned by postgres, and the files
# replace their objects as btracker_owner: drop them first, as the shape's
# superuser, and let btracker_owner create them again (the shape restores the roles).
# The query code may read table columns and tables db/btracker_app.sql defines
# after the shape's capture: add them, catalog only (nullable, no rewrite, no
# statistics), with HAF's event triggers on registered tables off (replica role).
"${shape_psql[@]}" -v ON_ERROR_STOP=1 -v db="$AIDEV_DB_SHAPE_DATABASE" -v schema="$schema" <<'SQL'
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS btracker_endpoints CASCADE;
DROP SCHEMA IF EXISTS btracker_backend CASCADE;
SET session_replication_role = replica;
ALTER TABLE :"schema".account_balance_history ADD COLUMN IF NOT EXISTS op_type_id SMALLINT;
ALTER TABLE :"schema".account_savings_history ADD COLUMN IF NOT EXISTS op_type_id SMALLINT;
CREATE TABLE IF NOT EXISTS :"schema".balance_totals
  (nai SMALLINT NOT NULL, holders BIGINT NOT NULL, total NUMERIC NOT NULL, CONSTRAINT pk_balance_totals PRIMARY KEY (nai));
CREATE TABLE IF NOT EXISTS :"schema".proposal_payment_totals
  (proposal_id INT NOT NULL, receiver INT NOT NULL, total_paid BIGINT NOT NULL, payment_count INT NOT NULL,
   first_payment_block INT NOT NULL, first_payment_at TIMESTAMP NOT NULL, last_payment_block INT NOT NULL, last_payment_at TIMESTAMP NOT NULL,
   CONSTRAINT pk_proposal_payment_totals PRIMARY KEY (proposal_id));
CREATE TABLE IF NOT EXISTS :"schema".proposal_payments
  (proposal_id INT NOT NULL, payment_seq_no INT NOT NULL, amount BIGINT NOT NULL, source_op BIGINT NOT NULL,
   CONSTRAINT pk_proposal_payments PRIMARY KEY (proposal_id, payment_seq_no));
RESET session_replication_role;
GRANT CREATE, CONNECT ON DATABASE :"db" TO btracker_owner, btracker_user;
GRANT ALL ON SCHEMA :"schema" TO btracker_owner;
GRANT SELECT ON ALL TABLES IN SCHEMA :"schema" TO btracker_owner;
SQL

# A shape holds no rows, but backend/shared/nai_types.sql refuses to install
# unless asset_table carries the three assets db/btracker_app.sql seeds it with:
# seed the copy's empty asset_table with that file's own INSERT.
seed="$(awk '/INSERT INTO asset_table/,/;/' db/btracker_app.sql)"
[ -n "$seed" ] || { echo "shape-apply.sh: no asset_table seed found in db/btracker_app.sql" >&2; exit 2; }
{
    printf 'SET SEARCH_PATH TO %s;\n' "$schema"
    printf 'SELECT NOT EXISTS (SELECT 1 FROM asset_table) AS asset_table_empty \\gset\n'
    printf '\\if :asset_table_empty\n%s\n\\endif\n' "$seed"
} | "${shape_psql[@]}" -v ON_ERROR_STOP=1

# The files install_app.sh runs, in its order, minus the ones above.
# shellcheck disable=SC2016  # $SCRIPTPATH is matched literally in install_app.sh's text
mapfile -t files < <(grep -oE '"\$SCRIPTPATH/\.\./[^"]+\.sql"' scripts/install_app.sh \
    | sed -E 's#^"\$SCRIPTPATH/\.\./##; s#"$##' \
    | grep -vE '^db/(btracker_app|builtin_roles|process_[a-z_]+)\.sql$')
[ "${#files[@]}" -gt 0 ] || { echo "shape-apply.sh: no SQL files found in scripts/install_app.sh" >&2; exit 2; }

for f in "${files[@]}"; do
    printf "SET custom.swagger_url = '{btracker-host}'; SET custom.is_forking = 'false'; SET SEARCH_PATH TO %s;\n" "$schema"
    cat "$f"
    printf '\n'
done | "${shape_psql[@]}" -v ON_ERROR_STOP=1
