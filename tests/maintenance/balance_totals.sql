/*
 * balance_totals.sql
 * ==================
 * balance_totals (holder count and balance sum per NAI, maintained by
 * process_block_range_balances) against the aggregates it replaces,
 * COUNT(*) FILTER (balance > 0) and SUM(balance) over current_account_balances:
 *
 *   - after the massive sync and finalize_massive_sync();
 *   - after a live block: synthetic operations in the synced head block that
 *     remove a HIVE holder, add an HBD holder and add VESTS, processed by
 *     process_block_range_balances in the LIVE path;
 *   - after HAF rolls that block back (hive.context_back_from_fork), which reverts
 *     the registered tables (skipped when the context is not forking).
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. The live block and its rollback run in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- balance_totals rows that differ from the aggregates over current_account_balances,
-- as 'nai: stored holders/total, aggregated holders/total'.
CREATE FUNCTION pg_temp.balance_totals_mismatches()
RETURNS TEXT[] LANGUAGE sql STABLE
AS $f$
  WITH agg AS (
    SELECT cab.nai, COUNT(*) FILTER (WHERE cab.balance > 0) AS holders, SUM(cab.balance::NUMERIC) AS total
    FROM current_account_balances cab
    GROUP BY cab.nai
  )
  SELECT coalesce(array_agg(format('%s: stored %s/%s, aggregated %s/%s',
                                   coalesce(bt.nai, agg.nai), bt.holders, bt.total, agg.holders, agg.total)
                            ORDER BY coalesce(bt.nai, agg.nai)), '{}')
  FROM balance_totals bt
  FULL JOIN agg ON agg.nai = bt.nai
  WHERE bt.holders IS DISTINCT FROM coalesce(agg.holders, 0)
     OR bt.total IS DISTINCT FROM coalesce(agg.total, 0)
$f$;

CREATE FUNCTION pg_temp.assert_balance_totals(_what TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __mismatches CONSTANT TEXT[] := pg_temp.balance_totals_mismatches();
BEGIN
  IF __mismatches <> '{}' THEN
    RAISE EXCEPTION 'balance_totals differs from current_account_balances %: %', _what, __mismatches;
  END IF;
END
$f$;

-- After the massive sync and finalize.
DO
$$
DECLARE
  __rows INT;
BEGIN
  PERFORM pg_temp.assert_balance_totals('after the massive sync');
  SELECT count(*) INTO __rows FROM balance_totals WHERE holders > 0;
  IF __rows <> 3 THEN
    RAISE EXCEPTION 'expected holders of HBD, HIVE and VESTS after the sync, found % NAIs with holders', __rows;
  END IF;
  RAISE NOTICE 'PASS: balance_totals equals the aggregates after the massive sync';
END
$$;

-- A live block, then its fork rollback.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __block CONSTANT INT := hive.app_get_current_block_num(__schema);
  __before JSONB;
  __after JSONB;
  __rolled_back JSONB;
  __hive_sender TEXT;
  __hive_sender_balance BIGINT;
  __hive_receiver TEXT;
  __hbd_sender TEXT;
  __hbd_receiver TEXT;
  __producer TEXT;
  __expected JSONB;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  SELECT jsonb_object_agg(nai, jsonb_build_array(holders, total)) INTO __before FROM balance_totals;

  -- A HIVE holder that sends its whole balance to another HIVE holder: one holder fewer.
  SELECT av.name, cab.balance INTO __hive_sender, __hive_sender_balance
  FROM current_account_balances cab JOIN hive.accounts_view av ON av.id = cab.account
  WHERE cab.nai = 21 AND cab.balance > 0
  ORDER BY cab.account LIMIT 1;
  SELECT av.name INTO __hive_receiver
  FROM current_account_balances cab JOIN hive.accounts_view av ON av.id = cab.account
  WHERE cab.nai = 21 AND cab.balance > 0 AND av.name <> __hive_sender
  ORDER BY cab.account LIMIT 1;
  -- An HBD holder that sends one unit to an account holding none: one holder more.
  SELECT av.name INTO __hbd_sender
  FROM current_account_balances cab JOIN hive.accounts_view av ON av.id = cab.account
  WHERE cab.nai = 13 AND cab.balance > 0
  ORDER BY cab.account LIMIT 1;
  SELECT av.name INTO __hbd_receiver
  FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM current_account_balances cab WHERE cab.account = av.id AND cab.nai = 13 AND cab.balance > 0)
  ORDER BY av.id LIMIT 1;
  -- A VESTS holder rewarded 1000 VESTS: the same holders, a larger total.
  SELECT av.name INTO __producer
  FROM current_account_balances cab JOIN hive.accounts_view av ON av.id = cab.account
  WHERE cab.nai = 37 AND cab.balance > 0
  ORDER BY cab.account LIMIT 1;

  IF __hive_receiver IS NULL OR __hbd_sender IS NULL OR __hbd_receiver IS NULL OR __producer IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts: % % % % %', __hive_sender, __hive_receiver, __hbd_sender, __hbd_receiver, __producer;
  END IF;

  -- Operations past the block's own ones (op_pos from 10000), so their ids sort after them.
  CREATE TEMP TABLE _btracker_ops_batch ON COMMIT DROP AS
  SELECT hafd.operation_id(__block, 10000 + f.pos) AS id, __block AS block_num, 0::SMALLINT AS trx_in_block,
         10000 + f.pos AS op_pos, ot.id AS op_type_id, f.op -> 'value' AS body_value, NULL::INT AS custom_json_type_id
  FROM (VALUES
    (1, jsonb_build_object('type', 'transfer_operation', 'value', jsonb_build_object(
          'from', __hive_sender, 'to', __hive_receiver, 'memo', '',
          'amount', jsonb_build_object('amount', __hive_sender_balance::TEXT, 'precision', 3, 'nai', '@@000000021')))),
    (2, jsonb_build_object('type', 'transfer_operation', 'value', jsonb_build_object(
          'from', __hbd_sender, 'to', __hbd_receiver, 'memo', '',
          'amount', jsonb_build_object('amount', '1', 'precision', 3, 'nai', '@@000000013')))),
    (3, jsonb_build_object('type', 'producer_reward_operation', 'value', jsonb_build_object(
          'producer', __producer,
          'vesting_shares', jsonb_build_object('amount', '1000', 'precision', 6, 'nai', '@@000000037'))))
  ) AS f(pos, op)
  JOIN hafd.operation_types ot ON ot.name = 'hive::protocol::' || (f.op ->> 'type');

  PERFORM process_block_range_balances(__block, __block);

  PERFORM pg_temp.assert_balance_totals('after a live block');
  SELECT jsonb_object_agg(nai, jsonb_build_array(holders, total)) INTO __after FROM balance_totals;
  __expected := jsonb_build_object(
    '13', jsonb_build_array((__before -> '13' ->> 0)::BIGINT + 1, (__before -> '13' ->> 1)::NUMERIC),
    '21', jsonb_build_array((__before -> '21' ->> 0)::BIGINT - 1, (__before -> '21' ->> 1)::NUMERIC),
    '37', jsonb_build_array((__before -> '37' ->> 0)::BIGINT,     (__before -> '37' ->> 1)::NUMERIC + 1000));
  IF __after IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'balance_totals after the live block: expected %, found % (before %)', __expected, __after, __before;
  END IF;
  RAISE NOTICE 'PASS: balance_totals follows a live block (% -> %)', __before, __after;

  IF NOT (SELECT hc.is_forking FROM hafd.contexts hc WHERE hc.name = __schema) THEN
    RAISE NOTICE 'SKIP: fork rollback: context % is not forking', __schema;
    RETURN;
  END IF;

  -- The synthetic block's changes were recorded at the context's current block.
  PERFORM hive.context_back_from_fork(__schema, __block - 1);

  PERFORM pg_temp.assert_balance_totals('after a fork rollback');
  SELECT jsonb_object_agg(nai, jsonb_build_array(holders, total)) INTO __rolled_back FROM balance_totals;
  IF __rolled_back IS DISTINCT FROM __before THEN
    RAISE EXCEPTION 'balance_totals after the fork rollback: expected %, found %', __before, __rolled_back;
  END IF;
  RAISE NOTICE 'PASS: a fork rollback reverts balance_totals with current_account_balances';
END
$$;
ROLLBACK;
