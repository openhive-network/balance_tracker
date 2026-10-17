/*
 * history_tables_vacuum.sql
 * =========================
 * Maintenance of the insert-only history tables (account_balance_history,
 * account_vesting_history) after a massive sync and finalize_massive_sync():
 *
 *   - both tables carry the insert-only autovacuum reloptions;
 *   - neither the massive sync nor finalize left a HAF vacuum request (HAF runs
 *     those as VACUUM FULL ANALYZE) for either table;
 *   - a MASSIVE_PROCESSING catch-up after finalize requests no vacuum, does not
 *     rewrite either table and does not ANALYZE it;
 *   - request_history_tables_freeze() asks for one 'freeze' request per table
 *     when HAF has the request kind (a test double records it when the installed
 *     HAF lacks it), and requests nothing when it does not.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case that changes state runs in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET stats_fetch_consistency = none;
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

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

-- The massive sync and finalize requested no vacuum of either table.
DO
$$
DECLARE
  __requests TEXT[];
BEGIN
  SELECT array_agg(table_name || ':' || status) INTO __requests
  FROM hafd.vacuum_requests
  WHERE schema_name = current_setting('btracker_test.schema')
    AND table_name IN ('account_balance_history', 'account_vesting_history');

  IF __requests IS NOT NULL THEN
    RAISE EXCEPTION 'vacuum requests left by the sync: %', __requests;
  END IF;
  RAISE NOTICE 'PASS: no vacuum request from the massive sync or finalize';
END
$$;

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
  __requests BIGINT;
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

  SELECT count(*) INTO __requests
  FROM hafd.vacuum_requests
  WHERE schema_name = __schema
    AND table_name IN ('account_balance_history', 'account_vesting_history');

  IF __requests <> 0 THEN
    RAISE EXCEPTION 'post-finalize catch-up requested % vacuum(s)', __requests;
  END IF;
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
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __has_kind CONSTANT BOOLEAN := EXISTS (
    SELECT 1 FROM pg_proc
    WHERE pronamespace = 'hive'::regnamespace AND proname = 'app_request_table_vacuum' AND pronargs = 4);
  __requested TEXT[];
BEGIN
  PERFORM request_history_tables_freeze(__schema);

  SELECT array_agg(table_name ORDER BY table_name) INTO __requested
  FROM hafd.vacuum_requests
  WHERE schema_name = __schema
    AND table_name IN ('account_balance_history', 'account_vesting_history');

  IF __has_kind AND __requested IS DISTINCT FROM ARRAY['account_balance_history', 'account_vesting_history'] THEN
    RAISE EXCEPTION 'HAF has the request kind, but the freeze requests are %', __requested;
  END IF;
  IF NOT __has_kind AND __requested IS NOT NULL THEN
    RAISE EXCEPTION 'HAF has no request kind, but vacuum requests were made: %', __requested;
  END IF;
  RAISE NOTICE 'PASS: freeze request on the installed HAF (request kind available: %)', __has_kind;
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
