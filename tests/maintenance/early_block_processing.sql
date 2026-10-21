/*
 * early_block_processing.sql
 * ==========================
 * Block processing while hived replays with psql-massive-index-policy =
 * keep-monotonic (hive/haf#379):
 *
 *   - the install opted the context into early block processing (skipped on a
 *     HAF without it, where the install skips the opt-in);
 *   - in the LIVE stage, an unfinalized, non-forking context keeps processing
 *     massively while HAF's own massive sync has not finished (its deferred
 *     core indexes are missing), and btracker_process_blocks() then does not
 *     finalize; it finalizes once HAF has finished, or when the context is
 *     already forking (finalized, or switched by an embedding application);
 *   - the MASSIVE_PROCESSING stage always processes massively.
 *
 * HAF's unfinished massive sync is the state hive.disable_indexes_of_irreversible
 * ('keep-monotonic') leaves: the 'keep' rows of hafd.indexes_constraints
 * 'created', the 'defer' rows 'missing'. Where the session may not write that
 * table (the shared HAF's consumer), a test double of haf_massive_sync_finished()
 * stands in for it.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case that changes state runs in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- Puts the context into the LIVE stage (the massive one with _live FALSE), as
-- hive.app_next_iteration's stage analysis does.
CREATE FUNCTION pg_temp.set_stage(_live BOOLEAN)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  UPDATE hafd.contexts
  SET loop.current_stage = CASE WHEN _live THEN hafd.live_stage() ELSE stages[1] END
  WHERE name = current_setting('btracker_test.schema');
END
$f$;

-- An unfinalized context, as the massive sync leaves it: without the app indexes
-- (isIndexesCreated) and non-forking.
CREATE FUNCTION pg_temp.unfinalize()
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  SET LOCAL ROLE btracker_owner;
  DROP INDEX idx_account_balance_nai_balance_idx;
  RESET ROLE;
  UPDATE hafd.contexts SET is_forking = FALSE WHERE name = current_setting('btracker_test.schema');
END
$f$;

-- HAF in its keep-monotonic massive sync: the deferred core indexes missing.
CREATE FUNCTION pg_temp.haf_in_massive_sync()
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  IF has_table_privilege('hafd.indexes_constraints', 'UPDATE') THEN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'hafd' AND table_name = 'indexes_constraints' AND column_name = 'massive_sync_class') THEN
      EXECUTE $u$ UPDATE hafd.indexes_constraints SET status = 'missing'
                  WHERE contexts @> ARRAY[0] AND massive_sync_class = 'defer' $u$;
      IF NOT hive.is_instance_ready_for_block_processing() THEN
        RAISE EXCEPTION 'expected HAF to be ready for block processing with only the defer indexes missing';
      END IF;
    ELSE
      UPDATE hafd.indexes_constraints SET status = 'missing'
      WHERE contexts @> ARRAY[0] AND table_name = 'hafd.account_operations';
    END IF;
    IF hive.is_instance_ready() THEN
      RAISE EXCEPTION 'expected HAF not to be ready with its deferred indexes missing';
    END IF;
    RAISE NOTICE 'HAF massive sync: deferred core indexes marked missing';
  ELSE
    SET LOCAL ROLE btracker_owner;
    CREATE OR REPLACE FUNCTION haf_massive_sync_finished()
    RETURNS BOOLEAN LANGUAGE sql STABLE AS $double$ SELECT FALSE $double$;
    RESET ROLE;
    RAISE NOTICE 'HAF massive sync: % may not write hafd.indexes_constraints, using a test double', current_user;
  END IF;
END
$f$;

CREATE FUNCTION pg_temp.assert_pending(_expected BOOLEAN, _what TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  IF is_massive_sync_pending(current_setting('btracker_test.schema')) IS DISTINCT FROM _expected THEN
    RAISE EXCEPTION '%: expected is_massive_sync_pending = %', _what, _expected;
  END IF;
  RAISE NOTICE 'PASS: % -> %', _what, CASE WHEN _expected THEN 'massive' ELSE 'finalize and live' END;
END
$f$;

-- The opt-in.
DO
$$
DECLARE
  __opted_in BOOLEAN;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'hafd' AND table_name = 'contexts' AND column_name = 'early_block_processing') THEN
    IF to_regproc('hive.app_context_set_early_block_processing') IS NOT NULL THEN
      RAISE EXCEPTION 'HAF has hive.app_context_set_early_block_processing but no hafd.contexts.early_block_processing';
    END IF;
    RAISE NOTICE 'SKIP: early block processing opt-in: the installed HAF predates hive/haf#379';
    RETURN;
  END IF;

  EXECUTE 'SELECT early_block_processing FROM hafd.contexts WHERE name = $1'
    INTO __opted_in USING current_setting('btracker_test.schema');
  IF __opted_in IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'context % is not opted into early block processing (%)', current_setting('btracker_test.schema'), __opted_in;
  END IF;
  RAISE NOTICE 'PASS: context opted into early block processing';
END
$$;

-- LIVE stage during HAF's massive sync, unfinalized: one empty range past the
-- synced block is processed massively and the context is not finalized.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __next_block CONSTANT INT := hive.app_get_current_block_num(__schema) + 1;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;
  PERFORM pg_temp.set_stage(TRUE);
  PERFORM pg_temp.haf_in_massive_sync();
  PERFORM pg_temp.assert_pending(FALSE, 'LIVE, finalized, HAF in massive sync');

  PERFORM pg_temp.unfinalize();
  PERFORM pg_temp.assert_pending(TRUE, 'LIVE, unfinalized, HAF in massive sync');

  PERFORM btracker_process_blocks(__schema, (__next_block, __next_block)::hive.blocks_range, false);
  IF isIndexesCreated() OR hive.app_is_forking(__schema)
     OR current_setting('btracker.rollups_backfilled', true) IS NOT DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'btracker_process_blocks finalized the context during HAF''s massive sync (indexes %, forking %, rollups backfilled %)',
      isIndexesCreated(), hive.app_is_forking(__schema), current_setting('btracker.rollups_backfilled', true);
  END IF;
  RAISE NOTICE 'PASS: no finalize in the LIVE stage during HAF''s massive sync';

  UPDATE hafd.contexts SET is_forking = TRUE WHERE name = __schema;
  PERFORM pg_temp.assert_pending(FALSE, 'LIVE, unfinalized but forking, HAF in massive sync');
END
$$;
ROLLBACK;

-- After HAF's massive sync the LIVE stage finalizes; the MASSIVE_PROCESSING
-- stage never does.
BEGIN;
DO
$$
BEGIN
  IF NOT hive.is_instance_ready() THEN
    RAISE EXCEPTION 'expected the installed HAF to have finished its massive sync';
  END IF;
  PERFORM pg_temp.unfinalize();
  PERFORM pg_temp.set_stage(TRUE);
  PERFORM pg_temp.assert_pending(FALSE, 'LIVE, unfinalized, HAF done');
  PERFORM pg_temp.set_stage(FALSE);
  PERFORM pg_temp.assert_pending(TRUE, 'MASSIVE_PROCESSING, unfinalized, HAF done');
END
$$;
ROLLBACK;
