/*
 * btracker_backend.get_impacted_balances
 * =======================================
 * Pure SQL/JSONB reimplementation of hive.get_impacted_balances (the C++ visitor in
 * hive/libraries/protocol/forward_impacted.cpp). db/process_balances.sql calls this
 * instead of routing body_value through hafd._operation_from_jsonb + the C function;
 * the JSON-parse + variant-construct roundtrip dominated LIVE block-processing time
 * (issue #53, ~15-30x regression).
 *
 * Emits one hive.impacted_balances_return row per (account, asset) balance delta.
 * `amount` is signed: negative decrements the account's balance for that asset.
 *
 * Arguments:
 *   _op_name - operation type with the 'hive::protocol::' prefix stripped; matches
 *              balance_impacting_ops.type_name in process_balances.
 *   _body    - the operation's "value" object (ho.body_value).
 *   _is_hf01 - whether the operation is past hardfork 1. Before HF1, VESTS amounts are
 *              scaled x1_000_000 to compensate for the block-905693 vesting_shares_split
 *              that multiplied every account's VESTS by a million.
 *
 * Asset fields are {"amount","precision","nai"} objects parsed by parse_amount_object.
 * Zero-amount rows are dropped, matching the C++ emplace_back guard.
 *
 * This must stay byte-for-byte equivalent to the C++ visitor. The op set is exactly
 * hive.get_balance_impacting_operations(); operations whose C++ overload is empty
 * (clear_null_account_balance, escrow_approve, transfer_to_vesting,
 * transfer_from_savings, limit_order_cancel) intentionally have no branch and emit
 * nothing. tests/parity/impacted_balances_parity.sql guards against drift.
 */

SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_balances(
    _op_name TEXT,
    _body    JSONB,
    _is_hf01 BOOLEAN
)
RETURNS SETOF hive.impacted_balances_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  -- escrow_transfer is the only op that folds a fee into one of two per-symbol totals
  -- (hive_spent / hbd_spent), so it cannot use the generic (account, sign, asset) model.
  IF _op_name = 'escrow_transfer_operation' THEN
    RETURN QUERY
    WITH parsed AS (
      SELECT
        btracker_backend.parse_amount_object(_body -> 'hive_amount') AS hive_amt,
        btracker_backend.parse_amount_object(_body -> 'hbd_amount')  AS hbd_amt,
        btracker_backend.parse_amount_object(_body -> 'fee')         AS fee
    ),
    spent AS (
      SELECT
        (p.hive_amt).amount
          + (CASE WHEN (p.fee).asset_symbol_nai = (p.hive_amt).asset_symbol_nai THEN (p.fee).amount ELSE 0 END) AS hive_spent,
        (p.hive_amt).asset_precision  AS hive_precision,
        (p.hive_amt).asset_symbol_nai AS hive_nai,
        (p.hbd_amt).amount
          + (CASE WHEN (p.fee).asset_symbol_nai = (p.hbd_amt).asset_symbol_nai THEN (p.fee).amount ELSE 0 END) AS hbd_spent,
        (p.hbd_amt).asset_precision  AS hbd_precision,
        (p.hbd_amt).asset_symbol_nai AS hbd_nai
      FROM parsed p
    )
    SELECT (_body ->> 'from')::VARCHAR, (- s.hive_spent)::BIGINT, s.hive_precision, s.hive_nai
    FROM spent s WHERE s.hive_spent <> 0
    UNION ALL
    SELECT (_body ->> 'from')::VARCHAR, (- s.hbd_spent)::BIGINT, s.hbd_precision, s.hbd_nai
    FROM spent s WHERE s.hbd_spent <> 0;
    RETURN;
  END IF;

  -- Every other op maps to a set of (account, sign, asset-object) emissions. Each branch
  -- is pruned by a constant WHERE on _op_name, so parse_amount_object runs only for the
  -- handful of emissions belonging to the matched op.
  RETURN QUERY
  WITH emissions(account_name, sign, asset) AS (
    --- liquid transfers ---
              SELECT _body ->> 'from', -1, _body -> 'amount'        WHERE _op_name = 'transfer_operation'
    UNION ALL SELECT _body ->> 'to',    1, _body -> 'amount'        WHERE _op_name = 'transfer_operation'
    UNION ALL SELECT _body ->> 'from', -1, _body -> 'amount'        WHERE _op_name = 'transfer_to_savings_operation'
    UNION ALL SELECT _body ->> 'to',    1, _body -> 'amount'        WHERE _op_name = 'fill_transfer_from_savings_operation'
    UNION ALL SELECT _body ->> 'from', -1, _body -> 'amount'        WHERE _op_name = 'fill_recurrent_transfer_operation'
    UNION ALL SELECT _body ->> 'to',    1, _body -> 'amount'        WHERE _op_name = 'fill_recurrent_transfer_operation'

    --- market orders ---
    UNION ALL SELECT _body ->> 'owner', -1, _body -> 'amount_to_sell' WHERE _op_name IN ('limit_order_create_operation', 'limit_order_create2_operation')
    UNION ALL SELECT _body ->> 'seller', 1, _body -> 'amount_back'    WHERE _op_name = 'limit_order_cancelled_operation'
    UNION ALL SELECT _body ->> 'open_owner',    1, _body -> 'current_pays' WHERE _op_name = 'fill_order_operation'
    UNION ALL SELECT _body ->> 'current_owner', 1, _body -> 'open_pays'    WHERE _op_name = 'fill_order_operation'

    --- conversions ---
    UNION ALL SELECT _body ->> 'owner', -1, _body -> 'amount'            WHERE _op_name IN ('convert_operation', 'collateralized_convert_operation')
    UNION ALL SELECT _body ->> 'owner',  1, _body -> 'amount_out'        WHERE _op_name = 'fill_convert_request_operation'
    UNION ALL SELECT _body ->> 'owner',  1, _body -> 'excess_collateral' WHERE _op_name = 'fill_collateralized_convert_request_operation'
    UNION ALL SELECT _body ->> 'owner',  1, _body -> 'hbd_out'           WHERE _op_name = 'collateralized_convert_immediate_conversion_operation'

    --- escrow (release / approved / rejected; transfer handled above) ---
    UNION ALL SELECT _body ->> 'receiver', 1, _body -> 'hive_amount' WHERE _op_name = 'escrow_release_operation'
    UNION ALL SELECT _body ->> 'receiver', 1, _body -> 'hbd_amount'  WHERE _op_name = 'escrow_release_operation'
    UNION ALL SELECT _body ->> 'agent',    1, _body -> 'fee'         WHERE _op_name = 'escrow_approved_operation'
    UNION ALL SELECT _body ->> 'from',     1, _body -> 'hbd_amount'  WHERE _op_name = 'escrow_rejected_operation'
    UNION ALL SELECT _body ->> 'from',     1, _body -> 'hive_amount' WHERE _op_name = 'escrow_rejected_operation'
    UNION ALL SELECT _body ->> 'from',     1, _body -> 'fee'         WHERE _op_name = 'escrow_rejected_operation'

    --- block / witness / liquidity / interest rewards ---
    UNION ALL SELECT _body ->> 'worker',   1, _body -> 'reward'         WHERE _op_name = 'pow_reward_operation'
    UNION ALL SELECT _body ->> 'producer', 1, _body -> 'vesting_shares' WHERE _op_name = 'producer_reward_operation'
    UNION ALL SELECT _body ->> 'owner',    1, _body -> 'payout'         WHERE _op_name = 'liquidity_reward_operation'
    UNION ALL SELECT _body ->> 'owner',    1, _body -> 'interest'       WHERE _op_name = 'interest_operation' AND (_body ->> 'is_saved_into_hbd_balance')::BOOLEAN

    --- author / benefactor / curation rewards (skipped when deferred to claim_reward_balance) ---
    UNION ALL SELECT _body ->> 'author',     1, _body -> 'hbd_payout'     WHERE _op_name = 'author_reward_operation'             AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'author',     1, _body -> 'hive_payout'    WHERE _op_name = 'author_reward_operation'             AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'author',     1, _body -> 'vesting_payout' WHERE _op_name = 'author_reward_operation'             AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'benefactor', 1, _body -> 'hbd_payout'     WHERE _op_name = 'comment_benefactor_reward_operation' AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'benefactor', 1, _body -> 'hive_payout'    WHERE _op_name = 'comment_benefactor_reward_operation' AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'benefactor', 1, _body -> 'vesting_payout' WHERE _op_name = 'comment_benefactor_reward_operation' AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN
    UNION ALL SELECT _body ->> 'curator',    1, _body -> 'reward'         WHERE _op_name = 'curation_reward_operation'           AND NOT (_body ->> 'payout_must_be_claimed')::BOOLEAN

    --- claim reward balance ---
    UNION ALL SELECT _body ->> 'account', 1, _body -> 'reward_hive'  WHERE _op_name = 'claim_reward_balance_operation'
    UNION ALL SELECT _body ->> 'account', 1, _body -> 'reward_hbd'   WHERE _op_name = 'claim_reward_balance_operation'
    UNION ALL SELECT _body ->> 'account', 1, _body -> 'reward_vests' WHERE _op_name = 'claim_reward_balance_operation'

    --- vesting power-up / power-down completions ---
    UNION ALL SELECT _body ->> 'to_account',       1, _body -> 'deposited'              WHERE _op_name = 'fill_vesting_withdraw_operation'
    UNION ALL SELECT _body ->> 'from_account',    -1, _body -> 'withdrawn'              WHERE _op_name = 'fill_vesting_withdraw_operation'
    UNION ALL SELECT _body ->> 'to_account',       1, _body -> 'vesting_shares_received' WHERE _op_name = 'transfer_to_vesting_completed_operation'
    UNION ALL SELECT _body ->> 'from_account',    -1, _body -> 'hive_vested'             WHERE _op_name = 'transfer_to_vesting_completed_operation'
    UNION ALL SELECT _body ->> 'new_account_name', 1, _body -> 'initial_vesting_shares'  WHERE _op_name = 'account_created_operation'

    --- account-creation fee burn (creator pays, null receives) ---
    UNION ALL SELECT _body ->> 'creator', -1, _body -> 'fee' WHERE _op_name IN ('account_create_operation', 'account_create_with_delegation_operation', 'claim_account_operation')
    UNION ALL SELECT 'null',               1, _body -> 'fee' WHERE _op_name IN ('account_create_operation', 'account_create_with_delegation_operation', 'claim_account_operation')

    --- DHF / proposals ---
    UNION ALL SELECT _body ->> 'receiver',  1, _body -> 'payment'          WHERE _op_name = 'proposal_pay_operation'
    UNION ALL SELECT _body ->> 'payer',    -1, _body -> 'payment'          WHERE _op_name = 'proposal_pay_operation'
    UNION ALL SELECT _body ->> 'creator',  -1, _body -> 'fee'              WHERE _op_name = 'proposal_fee_operation'
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'fee'              WHERE _op_name = 'proposal_fee_operation'
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'additional_funds' WHERE _op_name = 'dhf_funding_operation'
    UNION ALL SELECT _body ->> 'treasury', -1, _body -> 'hive_amount_in'   WHERE _op_name = 'dhf_conversion_operation'
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'hbd_amount_out'   WHERE _op_name = 'dhf_conversion_operation'

    --- hardfork balance migrations ---
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'hive_transferred'      WHERE _op_name = 'hardfork_hive_operation'
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'hbd_transferred'       WHERE _op_name = 'hardfork_hive_operation'
    UNION ALL SELECT _body ->> 'treasury',  1, _body -> 'total_hive_from_vests' WHERE _op_name = 'hardfork_hive_operation'
    UNION ALL SELECT _body ->> 'account',   1, _body -> 'hbd_transferred'       WHERE _op_name = 'hardfork_hive_restore_operation'
    UNION ALL SELECT _body ->> 'treasury', -1, _body -> 'hbd_transferred'       WHERE _op_name = 'hardfork_hive_restore_operation'
    UNION ALL SELECT _body ->> 'account',   1, _body -> 'hive_transferred'      WHERE _op_name = 'hardfork_hive_restore_operation'
    UNION ALL SELECT _body ->> 'treasury', -1, _body -> 'hive_transferred'      WHERE _op_name = 'hardfork_hive_restore_operation'

    --- treasury consolidation: total_moved is an array of assets ---
    UNION ALL SELECT 'hive.fund', 1, elem
              FROM jsonb_array_elements(_body -> 'total_moved') AS elem
              WHERE _op_name = 'consolidate_treasury_balance_operation'
  )
  SELECT
    e.account_name::VARCHAR,
    (e.sign * a.amount * (CASE WHEN a.asset_symbol_nai = 37 AND NOT _is_hf01 THEN 1000000 ELSE 1 END))::BIGINT,
    a.asset_precision,
    a.asset_symbol_nai
  FROM emissions e
  CROSS JOIN LATERAL btracker_backend.parse_amount_object(e.asset) AS a
  WHERE e.sign * a.amount * (CASE WHEN a.asset_symbol_nai = 37 AND NOT _is_hf01 THEN 1000000 ELSE 1 END) <> 0;
END
$$;

RESET ROLE;
