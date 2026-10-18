/*
 * history_op_type.sql
 * ===================
 * The op_type_id stored in account_balance_history and account_savings_history
 * after a massive sync:
 *
 *   - equals hafd.operations.op_type_id of the row's source_op, for every savings
 *     history row and a 1% page sample of the balance history;
 *   - is the operation's type for savings rows process_block_range_savings writes
 *     (synthetic transfer_to_savings and transfer_from_savings in the synced head
 *     block, in a transaction that is rolled back): savings start after the
 *     block range a test HAF may hold, which then has no savings history;
 *   - costs no tuple width: on the same sample a history row's columns take as
 *     many bytes with op_type_id as without it, whose 2-byte alignment pad it
 *     fills (HAF's hive_rowid, appended to both, is left out of both).
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced schema. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

DO
$$
DECLARE
  __checked BIGINT;
  __mismatched BIGINT;
  __example TEXT;
BEGIN
  SELECT count(*), count(*) FILTER (WHERE o.op_type_id IS DISTINCT FROM h.op_type_id),
         min(format('source_op %s: stored %s, operation %s', h.source_op, h.op_type_id, o.op_type_id))
           FILTER (WHERE o.op_type_id IS DISTINCT FROM h.op_type_id)
  INTO __checked, __mismatched, __example
  FROM account_balance_history h TABLESAMPLE SYSTEM (1) REPEATABLE (71)
  LEFT JOIN hafd.operations o ON o.id = h.source_op;
  IF __checked = 0 OR __mismatched <> 0 THEN
    RAISE EXCEPTION 'account_balance_history.op_type_id: % of % sampled rows differ from hafd.operations (%)',
      __mismatched, __checked, __example;
  END IF;
  RAISE NOTICE 'PASS: account_balance_history.op_type_id matches hafd.operations on % sampled rows', __checked;

  SELECT count(*), count(*) FILTER (WHERE o.op_type_id IS DISTINCT FROM h.op_type_id),
         min(format('source_op %s: stored %s, operation %s', h.source_op, h.op_type_id, o.op_type_id))
           FILTER (WHERE o.op_type_id IS DISTINCT FROM h.op_type_id)
  INTO __checked, __mismatched, __example
  FROM account_savings_history h
  LEFT JOIN hafd.operations o ON o.id = h.source_op;
  IF __checked = 0 THEN
    RAISE NOTICE 'SKIP: account_savings_history against hafd.operations: the synced range has no savings history';
  ELSIF __mismatched <> 0 THEN
    RAISE EXCEPTION 'account_savings_history.op_type_id: % of % rows differ from hafd.operations (%)',
      __mismatched, __checked, __example;
  ELSE
    RAISE NOTICE 'PASS: account_savings_history.op_type_id matches hafd.operations on all % rows', __checked;
  END IF;
END
$$;

-- Savings rows written by process_block_range_savings for a live block.
BEGIN;
DO
$$
DECLARE
  __block CONSTANT INT := hive.app_get_current_block_num(current_setting('btracker_test.schema'));
  __account TEXT;
  __expected TEXT[];
  __actual TEXT[];
BEGIN
  SELECT av.name INTO __account
  FROM current_account_balances cab JOIN hive.accounts_view av ON av.id = cab.account
  WHERE cab.nai = 21 AND cab.balance > 1000
  ORDER BY cab.account LIMIT 1;

  CREATE TEMP TABLE _btracker_ops_batch ON COMMIT DROP AS
  SELECT hafd.operation_id(__block, 10000 + f.pos) AS id, __block AS block_num, 0::SMALLINT AS trx_in_block,
         10000 + f.pos AS op_pos, ot.id AS op_type_id, f.op -> 'value' AS body_value, NULL::INT AS custom_json_type_id
  FROM (VALUES
    (1, jsonb_build_object('type', 'transfer_to_savings_operation', 'value', jsonb_build_object(
          'from', __account, 'to', __account, 'memo', '',
          'amount', jsonb_build_object('amount', '1000', 'precision', 3, 'nai', '@@000000021')))),
    (2, jsonb_build_object('type', 'transfer_from_savings_operation', 'value', jsonb_build_object(
          'from', __account, 'to', __account, 'memo', '', 'request_id', 7101,
          'amount', jsonb_build_object('amount', '400', 'precision', 3, 'nai', '@@000000021'))))
  ) AS f(pos, op)
  JOIN hafd.operation_types ot ON ot.name = 'hive::protocol::' || (f.op ->> 'type');

  SELECT array_agg(format('%s:%s', b.id, b.op_type_id) ORDER BY b.id) INTO __expected FROM _btracker_ops_batch b;

  PERFORM process_block_range_savings(__block, __block);

  SELECT array_agg(format('%s:%s', h.source_op, h.op_type_id) ORDER BY h.source_op) INTO __actual
  FROM account_savings_history h
  WHERE h.source_op IN (SELECT b.id FROM _btracker_ops_batch b);
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'account_savings_history rows of the live block (source_op:op_type_id): expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: process_block_range_savings stores each operation''s op_type_id (%)', __actual;
END
$$;
ROLLBACK;

DO
$$
DECLARE
  __with_column NUMERIC;
  __without_column NUMERIC;
BEGIN
  SELECT avg(pg_column_size(ROW(h.account, h.nai, h.op_type_id, h.balance, h.balance_seq_no, h.source_op))),
         avg(pg_column_size(ROW(h.account, h.nai, h.balance, h.balance_seq_no, h.source_op)))
  INTO __with_column, __without_column
  FROM account_balance_history h TABLESAMPLE SYSTEM (1) REPEATABLE (71);
  IF __with_column IS DISTINCT FROM __without_column THEN
    RAISE EXCEPTION 'account_balance_history rows are % bytes wide with op_type_id, % without', __with_column, __without_column;
  END IF;
  -- The measured layout is the tables' own: op_type_id right after nai.
  IF EXISTS (
    SELECT 1
    FROM unnest(ARRAY['account_balance_history', 'account_savings_history']::regclass[]) t
    WHERE (SELECT a.attnum FROM pg_attribute a WHERE a.attrelid = t AND a.attname = 'op_type_id')
          IS DISTINCT FROM (SELECT a.attnum + 1 FROM pg_attribute a WHERE a.attrelid = t AND a.attname = 'nai')) THEN
    RAISE EXCEPTION 'op_type_id does not follow nai in both history tables';
  END IF;
  RAISE NOTICE 'PASS: op_type_id adds no width to account_balance_history rows (% bytes)', __with_column;
END
$$;
