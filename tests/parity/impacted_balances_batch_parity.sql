/*
 * impacted_balances_batch_parity.sql
 * ==================================
 * Drift guard for btracker_backend.get_impacted_balances_batch (the set-based
 * variant of get_impacted_balances used by process_block_range_balances during
 * sync). For a fixture batch covering every emission family it asserts the batch
 * function's output equals, as a MULTISET, the per-row function applied to each
 * operation individually. The per-row function is itself parity-checked against
 * the C++ visitor (impacted_balances_parity.sql), so equality here keeps the
 * chain intact. Fixtures include: plain transfers, escrow_transfer with the fee
 * in HIVE and in HBD (the fee-folding special case that must stay combined into
 * per-symbol totals), the consolidate_treasury array form, a VESTS emission on
 * both sides of the HF01 boundary, and a zero-amount emission that must be
 * dropped by both implementations.
 *
 * Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on

DO
$$
DECLARE
  __hf01_block INT := 1000;
  __batch_minus_perrow BIGINT;
  __perrow_minus_batch BIGINT;
  __rows BIGINT;
BEGIN
  CREATE TEMP TABLE _btracker_ops_batch(
    id BIGINT, block_num INT, trx_in_block SMALLINT, op_pos INT,
    op_type_id SMALLINT, body_value JSONB, custom_json_type_id INT
  ) ON COMMIT DROP;

  INSERT INTO _btracker_ops_batch(id, block_num, trx_in_block, op_pos, op_type_id, body_value, custom_json_type_id)
  SELECT
    row_number() OVER () AS id,
    f.block_num,
    0::SMALLINT,
    0,
    ot.id,
    f.op::jsonb -> 'value',
    NULL
  FROM (VALUES
    -- (block_num, op json) ; hf01 boundary at block 1000
    (500,  '{"type":"transfer_operation","value":{"from":"admin","to":"steemit","amount":{"amount":"833000","precision":3,"nai":"@@000000021"},"memo":""}}'),
    (1500, '{"type":"transfer_operation","value":{"from":"alice","to":"bob","amount":{"amount":"1","precision":3,"nai":"@@000000013"},"memo":""}}'),
    -- zero amount: must be dropped by both implementations
    (1500, '{"type":"transfer_operation","value":{"from":"alice","to":"bob","amount":{"amount":"0","precision":3,"nai":"@@000000021"},"memo":""}}'),
    -- VESTS before HF01 (x1000000 scaling) and after
    (500,  '{"type":"producer_reward_operation","value":{"producer":"initminer","vesting_shares":{"amount":"1000","precision":6,"nai":"@@000000037"}}}'),
    (1500, '{"type":"producer_reward_operation","value":{"producer":"initminer","vesting_shares":{"amount":"1000","precision":6,"nai":"@@000000037"}}}'),
    -- escrow_transfer, fee in HIVE (folds into hive leg) and fee in HBD (folds into hbd leg)
    (1500, '{"type":"escrow_transfer_operation","value":{"from":"siol","to":"james","agent":"fabien","escrow_id":23456789,"hbd_amount":{"amount":"1000","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"7000","precision":3,"nai":"@@000000021"},"fee":{"amount":"50","precision":3,"nai":"@@000000021"},"ratification_deadline":"2017-02-26T11:22:39","escrow_expiration":"2017-02-28T11:22:39","json_meta":"{}"}}'),
    (1500, '{"type":"escrow_transfer_operation","value":{"from":"siol","to":"james","agent":"fabien","escrow_id":23456790,"hbd_amount":{"amount":"1000","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"7000","precision":3,"nai":"@@000000021"},"fee":{"amount":"50","precision":3,"nai":"@@000000013"},"ratification_deadline":"2017-02-26T11:22:39","escrow_expiration":"2017-02-28T11:22:39","json_meta":"{}"}}'),
    -- interest saved / not saved into hbd balance
    (1500, '{"type":"interest_operation","value":{"owner":"mr.agsexplorer","interest":{"amount":"3","precision":3,"nai":"@@000000013"},"is_saved_into_hbd_balance":true}}'),
    (1500, '{"type":"interest_operation","value":{"owner":"mr.agsexplorer","interest":{"amount":"3","precision":3,"nai":"@@000000013"},"is_saved_into_hbd_balance":false}}'),
    -- rewards deferred to claim (must emit nothing) and immediate
    (1500, '{"type":"author_reward_operation","value":{"author":"a","permlink":"p","hbd_payout":{"amount":"1","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"2","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"3","precision":6,"nai":"@@000000037"},"curators_vesting_payout":{"amount":"4","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":true}}'),
    (1500, '{"type":"author_reward_operation","value":{"author":"a","permlink":"p","hbd_payout":{"amount":"1","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"2","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"3","precision":6,"nai":"@@000000037"},"curators_vesting_payout":{"amount":"4","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":false}}'),
    -- account creation fee burn (two-sided emission incl. literal null account)
    (1500, '{"type":"account_create_operation","value":{"fee":{"amount":"3000","precision":3,"nai":"@@000000021"},"creator":"steem","new_account_name":"x","owner":{"weight_threshold":1,"account_auths":[],"key_auths":[]},"active":{"weight_threshold":1,"account_auths":[],"key_auths":[]},"posting":{"weight_threshold":1,"account_auths":[],"key_auths":[]},"memo_key":"STM1111111111111111111111111111111114T1Anm","json_metadata":""}}'),
    -- treasury consolidation array form
    (1500, '{"type":"consolidate_treasury_balance_operation","value":{"total_moved":[{"amount":"100","precision":3,"nai":"@@000000021"},{"amount":"200","precision":3,"nai":"@@000000013"}]}}')
  ) AS f(block_num, op)
  JOIN hafd.operation_types ot ON ot.name = 'hive::protocol::' || (f.op::jsonb ->> 'type');

  SELECT count(*) INTO __rows FROM _btracker_ops_batch;
  ASSERT __rows = 13, FORMAT('fixture setup incomplete: %s of 13 operation types resolved', __rows);

  WITH per_row AS MATERIALIZED (
    SELECT ho.id AS source_op, ho.block_num AS source_op_block, ho.trx_in_block,
           gib.account_name, gib.amount, gib.asset_precision, gib.asset_symbol_nai
    FROM _btracker_ops_batch ho
    JOIN hafd.operation_types ot ON ot.id = ho.op_type_id
    CROSS JOIN LATERAL btracker_backend.get_impacted_balances(
      replace(ot.name, 'hive::protocol::', ''), ho.body_value, ho.block_num > __hf01_block
    ) AS gib
  ), batch AS MATERIALIZED (
    SELECT * FROM btracker_backend.get_impacted_balances_batch(0, 2000000000, __hf01_block)
  )
  SELECT
    (SELECT count(*) FROM (SELECT * FROM batch EXCEPT ALL SELECT * FROM per_row) x),
    (SELECT count(*) FROM (SELECT * FROM per_row EXCEPT ALL SELECT * FROM batch) x)
  INTO __batch_minus_perrow, __perrow_minus_batch;

  ASSERT __batch_minus_perrow = 0 AND __perrow_minus_batch = 0,
    FORMAT('batch/per-row impacted-balances divergence: %s rows only in batch, %s rows only in per-row',
           __batch_minus_perrow, __perrow_minus_batch);

  RAISE NOTICE 'impacted_balances_batch_parity: OK (13 fixture ops, multisets equal)';
END
$$;
