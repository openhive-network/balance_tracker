/*
 * history_tables_vacuum.sql
 * =========================
 * Maintenance of the insert-only history tables (account_balance_history,
 * account_vesting_history) after a massive sync and finalize_massive_sync():
 *
 *   - both tables carry the insert-only autovacuum reloptions;
 *   - the massive sync and finalize left no HAF vacuum request for either table
 *     other than finalize's one 'freeze' request per table (none at all on a HAF
 *     without request kinds, where every request runs as VACUUM FULL ANALYZE);
 *   - a MASSIVE_PROCESSING catch-up after finalize adds no vacuum request, does
 *     not rewrite either table and does not ANALYZE it;
 *   - request_history_tables_freeze() asks for one 'freeze' request per table
 *     when HAF has the request kind (a test double records it when the installed
 *     HAF lacks it), and requests nothing when it does not;
 *   - a leftover 'full' request for a history table fails the request check.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case that changes state runs in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET stats_fetch_consistency = none;
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- Vacuum requests for the history tables as sorted 'table:kind' pairs. A HAF
-- without hafd.vacuum_requests.kind runs every request as VACUUM FULL, so its
-- requests read as kind 'full'.
CREATE FUNCTION pg_temp.history_vacuum_requests()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  SELECT coalesce(array_agg(p.pair ORDER BY p.pair), '{}')
  FROM hafd.vacuum_requests r
  CROSS JOIN LATERAL (SELECT r.table_name || ':' || coalesce(to_jsonb(r) ->> 'kind', 'full') AS pair) p
  WHERE r.schema_name = current_setting('btracker_test.schema')
    AND r.table_name IN ('account_balance_history', 'account_vesting_history')
$f$;

-- Detected as request_history_tables_freeze() does.
CREATE FUNCTION pg_temp.haf_has_request_kind()
RETURNS BOOLEAN LANGUAGE sql STABLE
AS $f$
  SELECT EXISTS (
    SELECT 1 FROM pg_proc
    WHERE pronamespace = 'hive'::regnamespace AND proname = 'app_request_table_vacuum' AND pronargs = 4)
$f$;

-- The requests finalize_massive_sync leaves: one 'freeze' per table when HAF
-- has the request kind, none when it does not.
CREATE FUNCTION pg_temp.requests_after_finalize()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  SELECT CASE WHEN pg_temp.haf_has_request_kind()
    THEN ARRAY['account_balance_history:freeze', 'account_vesting_history:freeze']
    ELSE '{}'::TEXT[] END
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

-- A leftover VACUUM FULL request fails the check above.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
BEGIN
  IF pg_temp.haf_has_request_kind() THEN
    EXECUTE 'SELECT hive.app_request_table_vacuum($1, $2, NULL::INTERVAL, ''full'')'
      USING __schema, 'account_balance_history';
  ELSE
    PERFORM hive.app_request_table_vacuum(__schema, 'account_balance_history', NULL::INTERVAL);
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
-- synced block.
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
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;
  IF hive.get_current_stage_name(__schema) IS DISTINCT FROM 'MASSIVE_PROCESSING' THEN
    RAISE EXCEPTION 'expected stage MASSIVE_PROCESSING, found %', hive.get_current_stage_name(__schema);
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
    format('%s.account_vesting_history/none/freeze', __schema)];

  IF __recorded IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'expected freeze requests %, recorded %', __expected, __recorded;
  END IF;
  RAISE NOTICE 'PASS: one unthrottled freeze request per history table';
END
$$;
ROLLBACK;
