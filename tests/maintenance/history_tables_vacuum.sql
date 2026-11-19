/*
 * history_tables_vacuum.sql
 * =========================
 * Maintenance of the insert-only history tables (account_balance_history,
 * account_vesting_history) after a massive sync and finalize_massive_sync():
 *
 *   - both tables carry the insert-only autovacuum reloptions;
 *   - the massive sync and finalize left no pending HAF vacuum request for either
 *     table or account_savings_history other than finalize's one 'freeze'
 *     request per table (none at all on a HAF without request kinds, where every
 *     request runs as VACUUM FULL ANALYZE); a VACUUM FULL request HAF already carried out, as an install
 *     synced before an upgrade keeps, is not a leftover;
 *   - a MASSIVE_PROCESSING catch-up after finalize adds no vacuum request, does
 *     not rewrite either table and does not ANALYZE it (skipped when the context
 *     has left that stage);
 *   - request_history_tables_freeze() asks for one 'freeze' request per table
 *     (account_savings_history too, which finalize also rewrites) when HAF has
 *     the request kind (a test double records it when the installed HAF lacks
 *     it), and requests nothing when it does not;
 *   - a leftover 'full' request for a history table fails the request check, also
 *     next to a carried-out one.
 *
 *   - retry_history_tables_freeze() (when finalize recorded history_freeze_pending)
 *     waits while finalize's freeze requests are pending and while another
 *     connection holds a snapshot older than the rewrite (a real second
 *     connection, opened from BTRACKER_TEST_DB_URL; skipped without it), and
 *     requests the freeze again once the horizon has passed;
 *   - after finalize's freeze requests are carried out as HAF runs them, at least
 *     95% of the pages of each non-empty history table are all-visible, and the
 *     retry stops without requesting anything.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case that changes state runs in a
 * transaction that is rolled back, except the last, which carries out
 * finalize's freeze requests (the VACUUM HAF would run) and lets the retry drop
 * history_freeze_pending. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET stats_fetch_consistency = none;
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- Vacuum requests for the history tables as sorted 'table:kind' pairs. A HAF
-- without hafd.vacuum_requests.kind runs every request as VACUUM FULL, so its
-- requests read as kind 'full'. A 'full' request counts only while pending: one
-- HAF already ran is history (an install's original massive sync leaves one per
-- table), not a leftover. Other kinds count whatever their status, since HAF may
-- already have run finalize's freeze.
CREATE FUNCTION pg_temp.history_vacuum_requests()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  SELECT coalesce(array_agg(r.table_name || ':' || k.kind ORDER BY r.table_name || ':' || k.kind), '{}')
  FROM hafd.vacuum_requests r
  CROSS JOIN LATERAL (SELECT coalesce(to_jsonb(r) ->> 'kind', 'full') AS kind) k
  WHERE r.schema_name = current_setting('btracker_test.schema')
    AND r.table_name IN ('account_balance_history', 'account_savings_history', 'account_vesting_history')
    AND (k.kind <> 'full' OR r.status = 'requested')
$f$;

-- Status of the history table's 'full' request, NULL when there is none.
CREATE FUNCTION pg_temp.full_request_status(_table_name TEXT)
RETURNS TEXT LANGUAGE sql STABLE
AS $f$
  SELECT r.status::TEXT
  FROM hafd.vacuum_requests r
  WHERE r.schema_name = current_setting('btracker_test.schema')
    AND r.table_name = _table_name
    AND coalesce(to_jsonb(r) ->> 'kind', 'full') = 'full'
$f$;

-- Detected as request_history_tables_freeze() does.
CREATE FUNCTION pg_temp.haf_has_request_kind()
RETURNS BOOLEAN LANGUAGE sql STABLE
AS $f$
  SELECT EXISTS (
    SELECT 1 FROM pg_proc
    WHERE pronamespace = 'hive'::regnamespace AND proname = 'app_request_table_vacuum' AND pronargs = 4)
$f$;

-- The requests finalize_massive_sync leaves: one 'freeze' per table it
-- rewrites when HAF has the request kind, none when it does not.
CREATE FUNCTION pg_temp.requests_after_finalize()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  SELECT CASE WHEN pg_temp.haf_has_request_kind()
    THEN ARRAY['account_balance_history:freeze', 'account_savings_history:freeze', 'account_vesting_history:freeze']
    ELSE '{}'::TEXT[] END
$f$;

-- A 'full' request HAF has carried out, as an install synced before an upgrade
-- keeps for each history table.
CREATE FUNCTION pg_temp.add_vacuumed_full_request(_table_name TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  IF pg_temp.haf_has_request_kind() THEN
    INSERT INTO hafd.vacuum_requests (schema_name, table_name, kind, status, last_vacuumed_time)
    VALUES (current_setting('btracker_test.schema'), _table_name, 'full', 'vacuumed', now())
    ON CONFLICT ON CONSTRAINT pk_hive_vacuum_requests DO UPDATE
    SET status = 'vacuumed', last_vacuumed_time = now(), error_message = NULL;
  ELSE
    INSERT INTO hafd.vacuum_requests (schema_name, table_name, status, last_vacuumed_time)
    VALUES (current_setting('btracker_test.schema'), _table_name, 'vacuumed', now())
    ON CONFLICT ON CONSTRAINT pk_hive_vacuum_requests DO UPDATE
    SET status = 'vacuumed', last_vacuumed_time = now(), error_message = NULL;
  END IF;
END
$f$;

CREATE FUNCTION pg_temp.assert_history_vacuum_requests(_expected TEXT[], _what TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __actual CONSTANT TEXT[] := pg_temp.history_vacuum_requests();
BEGIN
  IF __actual IS DISTINCT FROM _expected THEN
    RAISE EXCEPTION 'vacuum requests left by %: expected %, found %', _what, _expected, __actual;
  END IF;
END
$f$;

-- Reloptions, as both install paths leave them.
DO
$$
DECLARE
  __expected CONSTANT TEXT[][] := ARRAY[
    ['account_balance_history', 'autovacuum_vacuum_insert_scale_factor', '0'],
    ['account_balance_history', 'autovacuum_vacuum_insert_threshold',    '100000'],
    ['account_balance_history', 'autovacuum_freeze_min_age',             '0'],
    ['account_balance_history', 'autovacuum_analyze_scale_factor',       '0'],
    ['account_balance_history', 'autovacuum_analyze_threshold',          '1000000'],
    ['account_vesting_history', 'autovacuum_vacuum_insert_scale_factor', '0'],
    ['account_vesting_history', 'autovacuum_vacuum_insert_threshold',    '10000'],
    ['account_vesting_history', 'autovacuum_freeze_min_age',             '0'],
    ['account_vesting_history', 'autovacuum_analyze_scale_factor',       '0'],
    ['account_vesting_history', 'autovacuum_analyze_threshold',          '100000']
  ];
  __i INT;
  __actual TEXT;
BEGIN
  FOR __i IN 1 .. array_length(__expected, 1) LOOP
    SELECT o.option_value INTO __actual
    FROM pg_class c, pg_options_to_table(c.reloptions) o
    WHERE c.oid = __expected[__i][1]::regclass AND o.option_name = __expected[__i][2];

    IF __actual IS DISTINCT FROM __expected[__i][3] THEN
      RAISE EXCEPTION '%.%: expected %, found %',
        __expected[__i][1], __expected[__i][2], __expected[__i][3], __actual;
    END IF;
  END LOOP;
  RAISE NOTICE 'PASS: insert-only reloptions on both history tables';
END
$$;

-- The massive sync and finalize requested nothing but finalize's freeze.
DO
$$
BEGIN
  PERFORM pg_temp.assert_history_vacuum_requests(pg_temp.requests_after_finalize(), 'the sync and finalize');
  RAISE NOTICE 'PASS: no vacuum request from the massive sync or finalize besides finalize''s freeze (request kind available: %)',
    pg_temp.haf_has_request_kind();
END
$$;

-- A VACUUM FULL request HAF has carried out is not a leftover.
BEGIN;
DO
$$
BEGIN
  PERFORM pg_temp.add_vacuumed_full_request('account_balance_history');
  PERFORM pg_temp.add_vacuumed_full_request('account_vesting_history');
  IF pg_temp.full_request_status('account_balance_history') IS DISTINCT FROM 'vacuumed'
     OR pg_temp.full_request_status('account_vesting_history') IS DISTINCT FROM 'vacuumed' THEN
    RAISE EXCEPTION 'expected a vacuumed full request per history table, found % and %',
      pg_temp.full_request_status('account_balance_history'), pg_temp.full_request_status('account_vesting_history');
  END IF;

  PERFORM pg_temp.assert_history_vacuum_requests(pg_temp.requests_after_finalize(), 'a carried-out full request');
  RAISE NOTICE 'PASS: a vacuumed full request is not a leftover';
END
$$;
ROLLBACK;

-- A leftover VACUUM FULL request fails the check above, also when the table
-- already has a carried-out one: HAF keeps one 'full' row per table, so the
-- injected request turns it back to pending.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
BEGIN
  PERFORM pg_temp.add_vacuumed_full_request('account_balance_history');

  IF pg_temp.haf_has_request_kind() THEN
    EXECUTE 'SELECT hive.app_request_table_vacuum($1, $2, NULL::INTERVAL, ''full'')'
      USING __schema, 'account_balance_history';
  ELSE
    PERFORM hive.app_request_table_vacuum(__schema, 'account_balance_history', NULL::INTERVAL);
  END IF;

  IF pg_temp.full_request_status('account_balance_history') IS DISTINCT FROM 'requested' THEN
    RAISE EXCEPTION 'expected the injected full request to be pending, found status %',
      pg_temp.full_request_status('account_balance_history');
  END IF;
  IF NOT 'account_balance_history:full' = ANY (pg_temp.history_vacuum_requests()) THEN
    RAISE EXCEPTION 'the injected full request is not among the counted requests: %', pg_temp.history_vacuum_requests();
  END IF;

  BEGIN
    PERFORM pg_temp.assert_history_vacuum_requests(pg_temp.requests_after_finalize(), 'the injected request');
  EXCEPTION WHEN raise_exception THEN
    RAISE NOTICE 'PASS: a leftover full request is detected (%)', SQLERRM;
    RETURN;
  END;
  RAISE EXCEPTION 'an injected full request went undetected: %', pg_temp.history_vacuum_requests();
END
$$;
ROLLBACK;

-- A MASSIVE_PROCESSING catch-up after finalize: one empty range past the
-- synced block. A context that has reached the live stage has no such catch-up.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __next_block CONSTANT INT := hive.app_get_current_block_num(__schema) + 1;
  __filenodes_before OID[];
  __filenodes_after OID[];
  __analyzed_before TIMESTAMPTZ[];
  __analyzed_after TIMESTAMPTZ[];
  __requests_before CONSTANT TEXT[] := pg_temp.history_vacuum_requests();
  __stage CONSTANT TEXT := hive.get_current_stage_name(__schema);
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;
  IF __stage IS DISTINCT FROM 'MASSIVE_PROCESSING' THEN
    RAISE NOTICE 'SKIP: post-finalize catch-up: the context is in stage %, not MASSIVE_PROCESSING', __stage;
    RETURN;
  END IF;

  SELECT array_agg(pg_relation_filenode(t) ORDER BY t),
         array_agg(GREATEST(s.last_analyze, s.last_autoanalyze) ORDER BY t)
  INTO __filenodes_before, __analyzed_before
  FROM unnest(ARRAY['account_balance_history', 'account_vesting_history']::regclass[]) t
  JOIN pg_stat_user_tables s ON s.relid = t;

  PERFORM btracker_process_blocks(__schema, (__next_block, __next_block)::hive.blocks_range, false);

  SELECT array_agg(pg_relation_filenode(t) ORDER BY t),
         array_agg(GREATEST(s.last_analyze, s.last_autoanalyze) ORDER BY t)
  INTO __filenodes_after, __analyzed_after
  FROM unnest(ARRAY['account_balance_history', 'account_vesting_history']::regclass[]) t
  JOIN pg_stat_user_tables s ON s.relid = t;

  PERFORM pg_temp.assert_history_vacuum_requests(__requests_before, 'the post-finalize catch-up');
  IF __filenodes_after IS DISTINCT FROM __filenodes_before THEN
    RAISE EXCEPTION 'post-finalize catch-up rewrote a history table: % -> %', __filenodes_before, __filenodes_after;
  END IF;
  IF __analyzed_after IS DISTINCT FROM __analyzed_before THEN
    RAISE EXCEPTION 'post-finalize catch-up analyzed a history table: % -> %', __analyzed_before, __analyzed_after;
  END IF;
  RAISE NOTICE 'PASS: post-finalize catch-up neither vacuums, rewrites nor analyzes';
END
$$;
ROLLBACK;

-- The freeze request on the installed HAF.
BEGIN;
DO
$$
BEGIN
  PERFORM request_history_tables_freeze(current_setting('btracker_test.schema'));
  PERFORM pg_temp.assert_history_vacuum_requests(pg_temp.requests_after_finalize(), 'request_history_tables_freeze()');
  RAISE NOTICE 'PASS: freeze request on the installed HAF (request kind available: %)', pg_temp.haf_has_request_kind();
END
$$;
ROLLBACK;

-- The freeze request against a HAF with the request kind: a test double of the
-- four-argument hive.app_request_table_vacuum records its arguments.
BEGIN;
DO
$$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_proc
    WHERE pronamespace = 'hive'::regnamespace AND proname = 'app_request_table_vacuum' AND pronargs = 4) THEN
    RAISE NOTICE 'SKIP: the installed HAF has the request kind; covered by the case above';
    RETURN;
  END IF;

  CREATE TEMP TABLE recorded_vacuum_requests(schema_name TEXT, table_name TEXT, min_interval INTERVAL, kind TEXT) ON COMMIT DROP;
  CREATE FUNCTION hive.app_request_table_vacuum(_schema_name TEXT, _table_name TEXT, _min_interval INTERVAL, _kind TEXT)
  RETURNS VOID LANGUAGE sql
  AS $double$ INSERT INTO recorded_vacuum_requests VALUES (_schema_name, _table_name, _min_interval, _kind) $double$;
  GRANT INSERT ON recorded_vacuum_requests TO PUBLIC;
END
$$;

DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __recorded TEXT[];
  __expected TEXT[];
BEGIN
  IF to_regclass('pg_temp.recorded_vacuum_requests') IS NULL THEN
    RETURN;
  END IF;

  PERFORM request_history_tables_freeze(__schema);

  SELECT array_agg(format('%s.%s/%s/%s', schema_name, table_name, coalesce(min_interval::TEXT, 'none'), kind) ORDER BY table_name)
  INTO __recorded
  FROM recorded_vacuum_requests;
  __expected := ARRAY[
    format('%s.account_balance_history/none/freeze', __schema),
    format('%s.account_savings_history/none/freeze', __schema),
    format('%s.account_vesting_history/none/freeze', __schema)];

  IF __recorded IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'expected freeze requests %, recorded %', __expected, __recorded;
  END IF;
  RAISE NOTICE 'PASS: one unthrottled freeze request per rewritten history table';
END
$$;
ROLLBACK;

-- retry_history_tables_freeze() (#136). Finalize records its own xid, which
-- bounds every history row's xmin, in history_freeze_pending when HAF has the request kind; the cases below need
-- it and are skipped without it.
SELECT to_regclass('history_freeze_pending') IS NOT NULL AS freeze_pending \gset

-- Statuses of the history tables' 'freeze' requests as sorted 'table:status' pairs.
CREATE FUNCTION pg_temp.freeze_request_statuses()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  SELECT coalesce(array_agg(r.table_name || ':' || r.status ORDER BY r.table_name), '{}')
  FROM hafd.vacuum_requests r
  WHERE r.schema_name = current_setting('btracker_test.schema')
    AND r.table_name IN ('account_balance_history', 'account_savings_history', 'account_vesting_history')
    AND to_jsonb(r) ->> 'kind' = 'freeze'
$f$;

-- What a freeze vacuum HAF ran while the horizon was held leaves: its requests
-- carried out, the visibility map still empty; and the rewrite's xid set to
-- _xid.
CREATE FUNCTION pg_temp.freeze_ran_too_early(_xid XID)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  UPDATE hafd.vacuum_requests
  SET status = 'vacuumed', last_vacuumed_time = now()
  WHERE schema_name = current_setting('btracker_test.schema')
    AND table_name IN ('account_balance_history', 'account_savings_history', 'account_vesting_history')
    AND kind = 'freeze';
  SET LOCAL ROLE btracker_owner;
  UPDATE history_freeze_pending SET rewrite_xid = _xid;
  RESET ROLE;
END
$f$;

CREATE FUNCTION pg_temp.retry_freeze()
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  SET LOCAL ROLE btracker_owner;
  PERFORM retry_history_tables_freeze(current_setting('btracker_test.schema'));
  RESET ROLE;
END
$f$;

-- Whether account_balance_history, which every synced range fills, has no
-- visibility map yet, as the key-order rewrite leaves it.
CREATE FUNCTION pg_temp.balance_history_unmapped()
RETURNS BOOLEAN LANGUAGE sql STABLE
AS $f$
  SELECT relallvisible < 0.95 * relpages FROM pg_class WHERE oid = 'account_balance_history'::regclass
$f$;

\if :freeze_pending

-- While finalize's freeze requests are pending the retry waits for them.
BEGIN;
DO
$$
DECLARE
  __statuses_before CONSTANT TEXT[] := pg_temp.freeze_request_statuses();
BEGIN
  IF NOT 'account_balance_history:requested' = ANY (__statuses_before) THEN
    RAISE NOTICE 'SKIP: retry with pending requests: HAF has already carried them out (%)', __statuses_before;
    RETURN;
  END IF;
  PERFORM pg_temp.retry_freeze();
  IF to_regclass('history_freeze_pending') IS NULL THEN
    RAISE EXCEPTION 'retry_history_tables_freeze() dropped history_freeze_pending while finalize''s requests were pending';
  END IF;
  IF pg_temp.freeze_request_statuses() IS DISTINCT FROM __statuses_before THEN
    RAISE EXCEPTION 'retry with pending requests changed them: % -> %', __statuses_before, pg_temp.freeze_request_statuses();
  END IF;
  RAISE NOTICE 'PASS: the retry waits while finalize''s freeze requests are pending';
END
$$;
ROLLBACK;

-- A freeze vacuum run while another backend holds a snapshot older than the
-- rewrite marks nothing all-visible: the retry waits while such a snapshot
-- exists, then requests the freeze again. The holder is a second connection,
-- opened from the URL run-checks.sh exports.
\set test_db_url `echo "$BTRACKER_TEST_DB_URL"`
SELECT :'test_db_url' <> '' AND pg_temp.balance_history_unmapped() AS horizon_case \gset
\if :horizon_case

\! PGAPPNAME=btracker_test_horizon_holder psql "$BTRACKER_TEST_DB_URL" -X -q -c 'BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; SELECT pg_sleep(120); COMMIT' > /dev/null 2>&1 &

-- Top-level DO: each COMMIT releases this session's snapshot, so its own xmin
-- never holds the horizon it is waiting on.
DO
$$
BEGIN
  FOR __i IN 1 .. 60 LOOP
    IF EXISTS (SELECT 1 FROM pg_stat_activity
               WHERE application_name = 'btracker_test_horizon_holder' AND backend_xmin IS NOT NULL) THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.5);
    COMMIT;
  END LOOP;
  RAISE EXCEPTION 'the horizon-holding connection did not take its snapshot within 30 s';
END
$$;

-- A committed xid newer than the holder's snapshot stands for the rewrite's.
SELECT pg_current_xact_id()::XID AS rewrite_xid \gset
SELECT set_config('btracker_test.rewrite_xid', :'rewrite_xid', false);

BEGIN;
DO
$$
DECLARE
  __xid CONSTANT XID := current_setting('btracker_test.rewrite_xid')::XID;
  __statuses TEXT[];
BEGIN
  PERFORM pg_temp.freeze_ran_too_early(__xid);
  __statuses := pg_temp.freeze_request_statuses();
  IF vacuum_horizon_passed(__xid) THEN
    RAISE EXCEPTION 'vacuum_horizon_passed(%) while a backend holds an older snapshot', __xid;
  END IF;
  PERFORM pg_temp.retry_freeze();
  IF to_regclass('history_freeze_pending') IS NULL THEN
    RAISE EXCEPTION 'retry_history_tables_freeze() dropped history_freeze_pending while the horizon was held';
  END IF;
  IF pg_temp.freeze_request_statuses() IS DISTINCT FROM __statuses THEN
    RAISE EXCEPTION 'retry under a held horizon requested a vacuum: % -> %', __statuses, pg_temp.freeze_request_statuses();
  END IF;
  RAISE NOTICE 'PASS: the retry waits while a snapshot older than the rewrite is held';
END
$$;
ROLLBACK;

SELECT count(pg_terminate_backend(pid)) AS holders_terminated
FROM pg_stat_activity WHERE application_name = 'btracker_test_horizon_holder';

DO
$$
BEGIN
  FOR __i IN 1 .. 600 LOOP
    PERFORM pg_stat_clear_snapshot();
    IF vacuum_horizon_passed(current_setting('btracker_test.rewrite_xid')::XID) THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.5);
    COMMIT;
  END LOOP;
  RAISE EXCEPTION 'the horizon did not pass xid % within 300 s of terminating the holder: %',
    current_setting('btracker_test.rewrite_xid'),
    (SELECT array_agg(format('%s/%s xmin %s xid %s', pid, backend_type, backend_xmin, backend_xid))
     FROM pg_stat_activity WHERE backend_xmin IS NOT NULL OR backend_xid IS NOT NULL);
END
$$;

BEGIN;
DO
$$
DECLARE
  __expected CONSTANT TEXT[] := ARRAY[
    'account_balance_history:requested', 'account_savings_history:requested', 'account_vesting_history:requested'];
BEGIN
  PERFORM pg_temp.freeze_ran_too_early(current_setting('btracker_test.rewrite_xid')::XID);
  PERFORM pg_temp.retry_freeze();
  IF pg_temp.freeze_request_statuses() IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'expected the retry to request the freeze again (%), found %', __expected, pg_temp.freeze_request_statuses();
  END IF;
  IF to_regclass('history_freeze_pending') IS NOT NULL THEN
    RAISE EXCEPTION 'retry_history_tables_freeze() kept history_freeze_pending after requesting the freeze again';
  END IF;
  PERFORM pg_temp.assert_history_vacuum_requests(pg_temp.requests_after_finalize(), 'the retry');
  RAISE NOTICE 'PASS: once the horizon passes the rewrite, the retry requests the freeze again and stops';
END
$$;
ROLLBACK;

\else
SELECT 'SKIP: retry under a held horizon: needs BTRACKER_TEST_DB_URL and a history table without a visibility map' AS skipped;
\endif

-- finalize's freeze requests carried out as HAF's executor runs them, outside
-- any transaction, after finalize committed: the visibility map ends up set, and
-- the next live block's retry finds nothing to request again. Not rolled back.
SET ROLE btracker_owner;
VACUUM (FREEZE, ANALYZE) account_balance_history, account_savings_history, account_vesting_history;
RESET ROLE;
UPDATE hafd.vacuum_requests
SET status = 'vacuumed', last_vacuumed_time = now()
WHERE schema_name = current_setting('btracker_test.schema')
  AND table_name IN ('account_balance_history', 'account_savings_history', 'account_vesting_history')
  AND kind = 'freeze' AND status = 'requested';

DO
$$
DECLARE
  __statuses TEXT[];
  __table TEXT;
  __pages INT;
  __all_visible INT;
BEGIN
  __statuses := pg_temp.freeze_request_statuses();
  PERFORM pg_temp.retry_freeze();
  IF to_regclass('history_freeze_pending') IS NOT NULL THEN
    RAISE EXCEPTION 'retry_history_tables_freeze() kept history_freeze_pending after the freeze vacuum ran';
  END IF;
  IF pg_temp.freeze_request_statuses() IS DISTINCT FROM __statuses THEN
    RAISE EXCEPTION 'the retry requested a vacuum after the freeze set the map: % -> %', __statuses, pg_temp.freeze_request_statuses();
  END IF;

  FOREACH __table IN ARRAY ARRAY['account_balance_history', 'account_savings_history', 'account_vesting_history'] LOOP
    SELECT relpages, relallvisible INTO __pages, __all_visible FROM pg_class WHERE oid = __table::regclass;
    IF __pages = 0 THEN
      RAISE NOTICE 'SKIP: % visibility map: the synced range left it empty', __table;
      CONTINUE;
    END IF;
    IF __all_visible < 0.95 * __pages THEN
      RAISE EXCEPTION '%: % of % pages all-visible after finalize''s freeze vacuum, expected at least 95%%',
        __table, __all_visible, __pages;
    END IF;
    RAISE NOTICE 'PASS: % of % pages of % all-visible after finalize''s freeze vacuum', __all_visible, __pages, __table;
  END LOOP;
  RAISE NOTICE 'PASS: the retry stops without a request once the freeze vacuum set the map';
END
$$;

\else
SELECT 'SKIP: retry_history_tables_freeze(): finalize recorded no history_freeze_pending (HAF has no request kind)' AS skipped;
\endif
