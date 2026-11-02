/*
 * indexes_created_per_schema.sql
 * ==============================
 * isIndexesCreated() answers for the schema it is installed in, with another schema
 * of the same database (a standalone btracker next to haf_block_explorer's hafbe_bal)
 * holding an index of the same name:
 *
 *   - this finalized schema is finalized whatever the other one holds;
 *   - with only the other schema's index built, this schema is not finalized.
 *
 * Expects psql variable `schema` (the application schema) and a synced, finalized
 * schema. Runs in a transaction that is rolled back. Run under
 * `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;

BEGIN;
DO
$$
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

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
  RAISE NOTICE 'PASS: isIndexesCreated() answers per schema';
END
$$;
ROLLBACK;
