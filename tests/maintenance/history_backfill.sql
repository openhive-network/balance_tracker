/*
 * history_backfill.sql
 * ====================
 * The upgrade backfill of account_delegation_history and account_withdraw_history
 * (repair delegation_and_withdraw_history, db/one_off_repairs.sql) on an install
 * synced before they existed. An upgrade at block U is simulated on the synced
 * schema: the history rows up to U deleted (the rows after U are the ones the new
 * block processing wrote after the upgrade) and the repair unrecorded.
 *
 *   - two schemas: with another schema holding a finalized index of the same name,
 *     isIndexesCreated() still answers for this one, and the repair on this
 *     unfinalized schema waits instead of running;
 *   - an upgrade during the massive sync: the repair does not raise and stays
 *     unrecorded; processing resumes; once the indexes exist, finalize's
 *     complete_one_off_repairs() runs it in chunks and records it, and the history
 *     equals the sync's, each (account, source_op) once;
 *   - a fork rollback after the backfill leaves its rows intact.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case runs in a transaction that is rolled
 * back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- The history the sync wrote.
CREATE TEMP TABLE _synced_delegations AS SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history;
CREATE TEMP TABLE _synced_withdraws AS SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history;

-- The upgrade block U: four fifths of the sync's history rows are at or before it.
CREATE TEMP TABLE _upgrade AS
SELECT hafd.operation_id_to_block_num(percentile_disc(0.8) WITHIN GROUP (ORDER BY h.source_op)) AS block
FROM (SELECT source_op FROM _synced_delegations UNION ALL SELECT source_op FROM _synced_withdraws) h;

CREATE FUNCTION pg_temp.upgrade_block()
RETURNS INT LANGUAGE sql
AS $f$
  SELECT block FROM _upgrade
$f$;

-- An install upgraded at U and processed on since: no history up to U, the repair
-- unrecorded. The rows are deleted with the tables' HAF triggers disabled, as the
-- earlier version never wrote them: a fork rollback must not bring them back.
CREATE FUNCTION pg_temp.upgrade_at_block()
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __upgrade_block CONSTANT INT := pg_temp.upgrade_block();
BEGIN
  SET LOCAL ROLE btracker_owner;
  ALTER TABLE account_delegation_history DISABLE TRIGGER USER;
  ALTER TABLE account_withdraw_history DISABLE TRIGGER USER;
  DELETE FROM account_delegation_history WHERE hafd.operation_id_to_block_num(source_op) <= __upgrade_block;
  DELETE FROM account_withdraw_history WHERE hafd.operation_id_to_block_num(source_op) <= __upgrade_block;
  ALTER TABLE account_delegation_history ENABLE TRIGGER USER;
  ALTER TABLE account_withdraw_history ENABLE TRIGGER USER;
  DELETE FROM one_off_repairs WHERE name = 'delegation_and_withdraw_history';
  DELETE FROM history_backfill;
  DELETE FROM history_backfill_delegation_pairs;
  RESET ROLE;
END
$f$;

CREATE FUNCTION pg_temp.is_recorded()
RETURNS BOOLEAN LANGUAGE sql
AS $f$
  SELECT EXISTS (SELECT 1 FROM one_off_repairs WHERE name = 'delegation_and_withdraw_history')
$f$;

CREATE FUNCTION pg_temp.history_rows()
RETURNS BIGINT LANGUAGE sql
AS $f$
  SELECT (SELECT count(*) FROM account_delegation_history) + (SELECT count(*) FROM account_withdraw_history)
$f$;

-- The history equals the sync's, and no (account, source_op) is in a table twice.
CREATE FUNCTION pg_temp.expect_synced_history(_case TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __differing BIGINT;
  __duplicated BIGINT;
BEGIN
  SELECT count(*) INTO __differing FROM (
    (SELECT * FROM _synced_delegations EXCEPT ALL SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history)
    UNION ALL
    (SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history EXCEPT ALL SELECT * FROM _synced_delegations)) x;
  IF __differing <> 0 THEN
    RAISE EXCEPTION '%: account_delegation_history differs from the sync''s in % rows', _case, __differing;
  END IF;
  SELECT count(*) INTO __differing FROM (
    (SELECT * FROM _synced_withdraws EXCEPT ALL SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history)
    UNION ALL
    (SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history EXCEPT ALL SELECT * FROM _synced_withdraws)) x;
  IF __differing <> 0 THEN
    RAISE EXCEPTION '%: account_withdraw_history differs from the sync''s in % rows', _case, __differing;
  END IF;

  SELECT count(*) INTO __duplicated FROM (
    SELECT account, source_op FROM account_delegation_history GROUP BY account, source_op HAVING count(*) > 1
    UNION ALL
    SELECT account, source_op FROM account_withdraw_history GROUP BY account, source_op HAVING count(*) > 1) d;
  IF __duplicated <> 0 THEN
    RAISE EXCEPTION '%: % (account, source_op) are in a history table more than once', _case, __duplicated;
  END IF;
END
$f$;

DO
$$
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;
  -- Delegations start at HF17, after the test chain's blocks: only power-downs.
  IF NOT EXISTS (SELECT 1 FROM _synced_withdraws) THEN
    RAISE EXCEPTION 'expected the sync to write account_withdraw_history';
  END IF;
  IF (SELECT count(*) FROM account_withdraw_history WHERE hafd.operation_id_to_block_num(source_op) > pg_temp.upgrade_block()) = 0 THEN
    RAISE EXCEPTION 'expected history rows after the upgrade block %', pg_temp.upgrade_block();
  END IF;
  PERFORM pg_temp.expect_synced_history('the sync');
  RAISE NOTICE 'PASS: the sync writes each (account, source_op) once; upgrade simulated at block %', pg_temp.upgrade_block();
END
$$;

-- Two schemas, only the other one finalized.
BEGIN;
DO
$$
DECLARE
  __rows BIGINT;
BEGIN
  CREATE SCHEMA btracker_test_other;
  CREATE TABLE btracker_test_other.current_account_balances (nai INT, balance BIGINT);
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'two schemas: this finalized schema is reported unfinalized';
  END IF;

  CREATE INDEX idx_account_balance_nai_balance_idx ON btracker_test_other.current_account_balances (nai, balance DESC);
  SET LOCAL ROLE btracker_owner;
  DROP INDEX idx_account_balance_nai_balance_idx;
  RESET ROLE;
  IF isIndexesCreated() THEN
    RAISE EXCEPTION 'two schemas: this unfinalized schema is reported finalized for the other schema''s index';
  END IF;

  PERFORM pg_temp.upgrade_at_block();
  __rows := pg_temp.history_rows();
  PERFORM run_one_off_repairs();
  IF pg_temp.is_recorded() OR one_off_repairs_in_progress() OR pg_temp.history_rows() <> __rows THEN
    RAISE EXCEPTION 'two schemas: the backfill ran on the unfinalized schema (recorded %, in progress %, rows % -> %)',
      pg_temp.is_recorded(), one_off_repairs_in_progress(), __rows, pg_temp.history_rows();
  END IF;
  RAISE NOTICE 'PASS: isIndexesCreated() answers per schema, and the unfinalized schema''s backfill waits';
END
$$;
ROLLBACK;

-- An upgrade during the massive sync, processing resumed, then finalize.
BEGIN;
DO
$$
DECLARE
  __rows BIGINT;
BEGIN
  PERFORM pg_temp.upgrade_at_block();
  SET LOCAL ROLE btracker_owner;
  DROP INDEX idx_account_balance_nai_balance_idx;
  RESET ROLE;

  __rows := pg_temp.history_rows();
  PERFORM run_one_off_repairs();
  PERFORM continue_one_off_repairs();
  IF pg_temp.is_recorded() OR one_off_repairs_in_progress() OR pg_temp.history_rows() <> __rows THEN
    RAISE EXCEPTION 'massive sync: the repair did not wait (recorded %, in progress %, rows % -> %)',
      pg_temp.is_recorded(), one_off_repairs_in_progress(), __rows, pg_temp.history_rows();
  END IF;

  -- finalize_massive_sync() builds the indexes, then completes the pending repairs.
  SET LOCAL ROLE btracker_owner;
  CREATE INDEX idx_account_balance_nai_balance_idx ON current_account_balances (nai, balance DESC);
  RESET ROLE;
  PERFORM complete_one_off_repairs();

  IF NOT pg_temp.is_recorded() OR one_off_repairs_in_progress() THEN
    RAISE EXCEPTION 'massive sync: finalize did not complete the repair (recorded %, in progress %)', pg_temp.is_recorded(), one_off_repairs_in_progress();
  END IF;
  IF (SELECT rows_repaired FROM one_off_repairs WHERE name = 'delegation_and_withdraw_history') <> pg_temp.history_rows() - __rows THEN
    RAISE EXCEPTION 'massive sync: the repair recorded % rows, it wrote %',
      (SELECT rows_repaired FROM one_off_repairs WHERE name = 'delegation_and_withdraw_history'), pg_temp.history_rows() - __rows;
  END IF;
  IF EXISTS (SELECT 1 FROM history_backfill_delegation_pairs) THEN
    RAISE EXCEPTION 'massive sync: the completed backfill left its delegation pairs';
  END IF;
  PERFORM pg_temp.expect_synced_history('massive sync');
  RAISE NOTICE 'PASS: an upgrade during the massive sync waits; finalize backfills % rows up to the resumed processing, each once', pg_temp.history_rows() - __rows;
END
$$;
ROLLBACK;

-- A fork after the backfill.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __rows BIGINT;
BEGIN
  IF NOT (SELECT hc.is_forking FROM hafd.contexts hc WHERE hc.name = __schema) THEN
    RAISE NOTICE 'SKIP: fork after the backfill: context % is not forking', __schema;
    RETURN;
  END IF;

  PERFORM pg_temp.upgrade_at_block();
  PERFORM run_one_off_repairs();
  PERFORM complete_one_off_repairs();
  IF NOT pg_temp.is_recorded() THEN
    RAISE EXCEPTION 'fork: the backfill did not complete on a finalized schema';
  END IF;
  __rows := pg_temp.history_rows();

  PERFORM hive.context_back_from_fork(__schema, hive.app_get_current_block_num(__schema) - 1);

  IF pg_temp.history_rows() <> __rows THEN
    RAISE EXCEPTION 'fork: a rollback of the current block changed the backfilled history from % to % rows', __rows, pg_temp.history_rows();
  END IF;
  PERFORM pg_temp.expect_synced_history('fork');
  RAISE NOTICE 'PASS: a fork rollback after the backfill leaves its % history rows', __rows;
END
$$;
ROLLBACK;
