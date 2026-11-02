SET ROLE btracker_owner;

/*
Balances at a past block, for the `at-block` parameter of the balances endpoints.
Called by: btracker_backend.account_balances_at()

The fields with stored history are read from it: the row with the highest
source_op at or before the block is the value at that block.
  - hbd_balance, hive_balance, vesting_shares: account_balance_history
  - hbd_savings, hive_savings: account_savings_history
  - delegated_vests, received_vests: account_delegation_history
  - vesting_withdraw_rate, to_withdraw, withdrawn: account_withdraw_history
  - vesting_balance_hive: vesting_shares at the block's vesting fund and shares
  - post_voting_power_vests: derived from the above as at the head
Every other field has no history and is NULL.

Each lookup is a backward scan of the table's (account, [nai,] block) index from
the block; force_generic_plan plans each statement once per session, as
btracker_backend.get_account_balances() does.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_balances_at_block(
    _account_id INT,
    _block_num INT
)
RETURNS btracker_backend.balance
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = 'force_generic_plan'
AS $BODY$
DECLARE
  result_row btracker_backend.balance;
  _vesting_shares BIGINT;
  _delegated_vests BIGINT;
  _received_vests BIGINT;
  _nai_hbd   CONSTANT SMALLINT := btracker_backend.nai_hbd();
  _nai_hive  CONSTANT SMALLINT := btracker_backend.nai_hive();
  _nai_vests CONSTANT SMALLINT := btracker_backend.nai_vests();
BEGIN
  -- Several changes of one operation (a transfer to self) share source_op;
  -- balance_seq_no orders them.
  SELECT
    COALESCE(MAX(h.balance) FILTER (WHERE n.nai = _nai_hbd), 0)::BIGINT,
    COALESCE(MAX(h.balance) FILTER (WHERE n.nai = _nai_hive), 0)::BIGINT,
    COALESCE(MAX(h.balance) FILTER (WHERE n.nai = _nai_vests), 0)::BIGINT
  INTO result_row.hbd_balance, result_row.hive_balance, _vesting_shares
  FROM (VALUES (_nai_hbd), (_nai_hive), (_nai_vests)) AS n(nai)
  CROSS JOIN LATERAL (
    SELECT abh.balance
    FROM account_balance_history abh
    WHERE abh.account = _account_id AND abh.nai = n.nai
      AND hafd.operation_id_to_block_num(abh.source_op) <= _block_num
    ORDER BY hafd.operation_id_to_block_num(abh.source_op) DESC, abh.balance_seq_no DESC
    LIMIT 1
  ) h;

  -- The rounding of get_account_balances(), at the block's exchange rate.
  SELECT
    COALESCE(
      (_vesting_shares * bv.total_vesting_fund_hive::NUMERIC / NULLIF(bv.total_vesting_shares::NUMERIC, 0))::BIGINT,
      0
    )
  INTO result_row.vesting_balance_hive
  FROM hive.blocks_view bv
  WHERE bv.num = _block_num;

  SELECT
    COALESCE(MAX(h.balance) FILTER (WHERE n.nai = _nai_hbd), 0)::BIGINT,
    COALESCE(MAX(h.balance) FILTER (WHERE n.nai = _nai_hive), 0)::BIGINT
  INTO result_row.hbd_savings, result_row.hive_savings
  FROM (VALUES (_nai_hbd), (_nai_hive)) AS n(nai)
  CROSS JOIN LATERAL (
    SELECT ash.balance
    FROM account_savings_history ash
    WHERE ash.account = _account_id AND ash.nai = n.nai
      AND hafd.operation_id_to_block_num(ash.source_op) <= _block_num
    ORDER BY hafd.operation_id_to_block_num(ash.source_op) DESC, ash.balance_seq_no DESC
    LIMIT 1
  ) h;

  SELECT
    COALESCE(adh.delegated_vests, 0)::BIGINT,
    COALESCE(adh.received_vests, 0)::BIGINT
  INTO _delegated_vests, _received_vests
  FROM (VALUES(1)) AS dummy(_)
  LEFT JOIN LATERAL (
    SELECT h.delegated_vests, h.received_vests
    FROM account_delegation_history h
    WHERE h.account = _account_id
      AND hafd.operation_id_to_block_num(h.source_op) <= _block_num
    ORDER BY hafd.operation_id_to_block_num(h.source_op) DESC, h.source_op DESC
    LIMIT 1
  ) adh ON TRUE;

  result_row.vesting_shares          := _vesting_shares;
  result_row.delegated_vests         := _delegated_vests;
  result_row.received_vests          := _received_vests;
  result_row.post_voting_power_vests := (_vesting_shares - _delegated_vests + _received_vests)::BIGINT;

  SELECT
    COALESCE(awh.vesting_withdraw_rate, 0)::BIGINT,
    COALESCE(awh.to_withdraw, 0)::BIGINT,
    COALESCE(awh.withdrawn, 0)::BIGINT
  INTO result_row.vesting_withdraw_rate, result_row.to_withdraw, result_row.withdrawn
  FROM (VALUES(1)) AS dummy(_)
  LEFT JOIN LATERAL (
    SELECT h.vesting_withdraw_rate, h.to_withdraw, h.withdrawn
    FROM account_withdraw_history h
    WHERE h.account = _account_id
      AND hafd.operation_id_to_block_num(h.source_op) <= _block_num
    ORDER BY hafd.operation_id_to_block_num(h.source_op) DESC, h.source_op DESC
    LIMIT 1
  ) awh ON TRUE;

  RETURN result_row;
END;
$BODY$;

/*
The block an `at-block` parameter names: a block number, or a timestamp
(resolved to the last block at or before it). NULL when the parameter is absent
or names the last block balance tracker has processed: both mean the head state,
which the current-state tables hold.

Raises when the parameter is neither a number nor a timestamp, and when the block
is before block 1 or after the last processed block.
*/
CREATE OR REPLACE FUNCTION btracker_backend.resolve_at_block(_at_block TEXT)
RETURNS INT
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
  __last_block INT;
  __block NUMERIC;
  __timestamp TIMESTAMP;
BEGIN
  IF _at_block IS NULL THEN
    RETURN NULL;
  END IF;

  __last_block := btracker_backend.last_synced_block();

  IF btrim(_at_block) ~ '^[+-]?[0-9]+$' THEN
    __block := btrim(_at_block)::NUMERIC;
  ELSE
    BEGIN
      __timestamp := _at_block::TIMESTAMP;
    EXCEPTION
      WHEN invalid_datetime_format OR datetime_field_overflow THEN
        RAISE EXCEPTION 'at-block must be a block number or a timestamp (YYYY-MM-DD HH:MI:SS): ''%'' is neither', _at_block;
    END;
    __block := btracker_backend.block_at_or_before(__timestamp);
  END IF;

  IF __block IS NULL OR __block < 1 OR __block > __last_block THEN
    RAISE EXCEPTION 'at-block must be between block 1 and block % (the last block balance tracker has processed): ''%'' is not', __last_block, _at_block;
  END IF;

  RETURN NULLIF(__block::INT, __last_block);
END;
$BODY$;

RESET ROLE;
