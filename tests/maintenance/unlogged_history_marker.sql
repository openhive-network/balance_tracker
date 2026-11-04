/*
 * unlogged_history_marker.sql
 * ===========================
 * raise_if_unlogged_history_lost(), the guard against continuing a massive sync
 * whose UNLOGGED history tables PostgreSQL crash recovery emptied:
 *
 *   - after finalize_massive_sync() the history tables are LOGGED and the marker
 *     is gone, so the guard has nothing to check;
 *   - an emptied marker (the state crash recovery leaves) with current balances
 *     present raises;
 *   - an emptied marker with no current balances (nothing processed yet) does not
 *     raise and sets the marker again.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. The crash states are made in a transaction that
 * is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

DO
$$
DECLARE
  __unlogged TEXT[];
BEGIN
  IF to_regclass('unlogged_history_marker') IS NOT NULL THEN
    RAISE EXCEPTION 'finalize_massive_sync left unlogged_history_marker in place';
  END IF;

  SELECT array_agg(relname::TEXT ORDER BY relname) INTO __unlogged
  FROM pg_class
  WHERE relnamespace = current_setting('btracker_test.schema')::regnamespace
    AND relpersistence = 'u';
  IF __unlogged IS NOT NULL THEN
    RAISE EXCEPTION 'UNLOGGED tables after finalize_massive_sync: %', __unlogged;
  END IF;

  PERFORM raise_if_unlogged_history_lost(current_setting('btracker_test.schema'));
  RAISE NOTICE 'PASS: after finalize the history tables are LOGGED and the guard has no marker to check';
END
$$;

BEGIN;

-- The role the block processor runs as, which owns the schema.
SET LOCAL ROLE btracker_owner;
CREATE UNLOGGED TABLE unlogged_history_marker (intact BOOLEAN NOT NULL);

DO
$$
DECLARE
  __raised BOOLEAN := FALSE;
  __message TEXT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM current_account_balances) THEN
    RAISE EXCEPTION 'the synced schema has no current_account_balances rows';
  END IF;

  BEGIN
    PERFORM raise_if_unlogged_history_lost(current_setting('btracker_test.schema'));
  EXCEPTION WHEN raise_exception THEN
    __raised := TRUE;
    __message := SQLERRM;
  END;

  IF NOT __raised THEN
    RAISE EXCEPTION 'an emptied marker with current balances present did not raise';
  END IF;
  IF __message NOT LIKE '%crash emptied the UNLOGGED account_balance_history and account_vesting_history%' THEN
    RAISE EXCEPTION 'unexpected message for an emptied marker: %', __message;
  END IF;
  RAISE NOTICE 'PASS: an emptied marker with current balances present raises';

  DELETE FROM current_account_balances;
  PERFORM raise_if_unlogged_history_lost(current_setting('btracker_test.schema'));
  IF (SELECT count(*) FROM unlogged_history_marker) <> 1 THEN
    RAISE EXCEPTION 'an emptied marker with nothing processed was not set again';
  END IF;
  RAISE NOTICE 'PASS: an emptied marker with no current balances is set again';
END
$$;

ROLLBACK;
