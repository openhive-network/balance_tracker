/*
 * finalize_refuses_unlogged.sql
 * =============================
 * finalize_massive_sync() refuses a schema with an UNLOGGED table, as an install
 * synced in part by the older code that kept the history UNLOGGED leaves it:
 *
 *   - with account_vesting_history set UNLOGGED and the schema made to look
 *     unfinalized (isIndexesCreated() replaced by one answering false), it
 *     raises, naming the table and the uninstall-and-sync-again instruction,
 *     before switching the context to forking and restoring the indexes.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Runs in a transaction that is rolled back. Run
 * under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;

BEGIN;
DO
$$
DECLARE
  __schema hive.context_name := current_schema();
  __raised BOOLEAN := FALSE;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  SET LOCAL ROLE btracker_owner;
  ALTER TABLE account_vesting_history SET UNLOGGED;
  CREATE OR REPLACE FUNCTION isIndexesCreated() RETURNS BOOLEAN LANGUAGE plpgsql STABLE AS
    $f$ BEGIN RETURN FALSE; END $f$;

  BEGIN
    PERFORM finalize_massive_sync(__schema);
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE '%UNLOGGED: account_vesting_history.%' OR SQLERRM NOT LIKE '%uninstall it and sync again%' THEN
      RAISE EXCEPTION 'unexpected error for an UNLOGGED history table: %', SQLERRM;
    END IF;
    __raised := TRUE;
  END;
  IF NOT __raised THEN
    RAISE EXCEPTION 'finalize_massive_sync() finalized a schema with account_vesting_history UNLOGGED';
  END IF;
  RESET ROLE;

  RAISE NOTICE 'PASS: finalize_massive_sync() refuses an UNLOGGED table, naming it and the reinstall';
END
$$;
ROLLBACK;
