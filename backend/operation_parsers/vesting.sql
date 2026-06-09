/*
 * Vesting operation parsers
 * =========================
 * Extract the vesting (power-up / power-down) impact of an operation from its JSONB
 * body, so the processing functions (db/process_vesting_stats.sql,
 * db/process_account_vesting_stats.sql) contain only flow/aggregation logic and never
 * reach into the operation body themselves. Mirrors the get_impacted_balances pattern
 * (backend/operation_parsers/impacted_balances.sql).
 *
 * Two views of the same three operations:
 *   - get_impacted_vesting   -> PER-ACCOUNT rows (account, kind, hive, vests). Encodes the
 *                               issue #54 attribution: a routed fill_vesting_withdraw is
 *                               split into the sender's power_down_fill (kind 3) and the
 *                               recipient's power_down_route_received (kind 4).
 *   - get_vesting_op_stat    -> GLOBAL one-row-per-op (kind, hive, vests), no account
 *                               fan-out and never kind 4.
 *
 * `_is_hf01` = the op is at/after hardfork 1. Pre-HF1 VESTS amounts are scaled x1_000_000
 * (block-905693 vesting_shares_split) via vests_precision_multiplier — same treatment as
 * process_withdrawals / get_impacted_balances. The fill's `deposited` asset precision tells
 * HIVE (3) from VESTS (6): auto_vest=false pays the recipient in HIVE, auto_vest=true in VESTS.
 */

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_vesting_return CASCADE;
CREATE TYPE btracker_backend.impacted_vesting_return AS
(
    account_name VARCHAR,
    kind         SMALLINT, -- btracker_backend.vesting_kind_*()
    hive_amount  BIGINT,   -- HIVE satoshi (x1000), 0 when n/a
    vests_amount NUMERIC   -- VESTS satoshi (x1e6 post-HF1), 0 when n/a
);

DROP TYPE IF EXISTS btracker_backend.vesting_op_stat_return CASCADE;
CREATE TYPE btracker_backend.vesting_op_stat_return AS
(
    kind         SMALLINT,
    hive_amount  BIGINT,
    vests_amount NUMERIC
);

-- Per-account vesting impact (issue #54 attribution baked in here, not in the processor).
CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_vesting(
    _op_name TEXT,    -- operation type, 'hive::protocol::' prefix stripped
    _body    JSONB,   -- operation "value" object (body_value)
    _is_hf01 BOOLEAN
)
RETURNS SETOF btracker_backend.impacted_vesting_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  _pre_hf1 INT := btracker_backend.vests_precision_multiplier(_is_hf01);
BEGIN
  -- transfer_to_vesting: power-up. The moved HIVE impacts BOTH `from` and `to`
  -- (collapsed to one when from = to by the caller's DISTINCT).
  IF _op_name = 'transfer_to_vesting_operation' THEN
    RETURN QUERY
      SELECT
        acc.name::VARCHAR,
        btracker_backend.vesting_kind_power_up(),
        ((_body -> 'amount' ->> 'amount')::BIGINT),
        0::NUMERIC
      FROM (VALUES (_body ->> 'from'), (_body ->> 'to')) AS acc(name)
      WHERE acc.name IS NOT NULL;

  -- withdraw_vesting: power-down start for `account`; VESTS scheduled to withdraw.
  -- Cancellations (vesting_shares = 0) emit nothing.
  ELSIF _op_name = 'withdraw_vesting_operation' THEN
    RETURN QUERY
      SELECT
        (_body ->> 'account')::VARCHAR,
        btracker_backend.vesting_kind_power_down_init(),
        0::BIGINT,
        ((_body -> 'vesting_shares' ->> 'amount')::NUMERIC * _pre_hf1)
      WHERE (_body -> 'vesting_shares' ->> 'amount')::BIGINT <> 0;

  -- fill_vesting_withdraw: weekly payout. Split into the sender (own power-down) and,
  -- only when routed, the recipient (received-from-route).
  ELSIF _op_name = 'fill_vesting_withdraw_operation' THEN
    RETURN QUERY
    WITH fill AS (
      SELECT
        _body ->> 'from_account'                     AS from_account,
        _body ->> 'to_account'                       AS to_account,
        (_body -> 'withdrawn' ->> 'amount')::NUMERIC  AS withdrawn_vests,
        (_body -> 'deposited' ->> 'amount')::BIGINT   AS deposited_amount,
        (_body -> 'deposited' ->> 'precision')::INT   AS deposited_precision -- 3 = HIVE, 6 = VESTS
    )
    -- from_account: power_down_fill. Keeps the full withdrawn VESTS; takes the deposited
    -- HIVE only for an OWN power-down (to = from) so routed-away HIVE is not credited here.
    SELECT
      f.from_account::VARCHAR,
      btracker_backend.vesting_kind_power_down_fill(),
      (CASE WHEN f.deposited_precision = 3 AND f.to_account = f.from_account
            THEN f.deposited_amount ELSE 0::BIGINT END),
      (f.withdrawn_vests * _pre_hf1)
    FROM fill f
    UNION ALL
    -- to_account: power_down_route_received, ONLY when routed (to <> from). The recipient
    -- gains the deposited asset: HIVE (auto_vest=false) or VESTS (auto_vest=true).
    SELECT
      f.to_account::VARCHAR,
      btracker_backend.vesting_kind_power_down_route_received(),
      (CASE WHEN f.deposited_precision = 3 THEN f.deposited_amount ELSE 0::BIGINT END),
      (CASE WHEN f.deposited_precision = 6 THEN f.deposited_amount::NUMERIC * _pre_hf1 ELSE 0::NUMERIC END)
    FROM fill f
    WHERE f.to_account <> f.from_account;
  END IF;
END
$$;

-- Global per-op vesting stat (no account dimension, never kind 4). The fill's HIVE column
-- is the realised deposited HIVE regardless of routing (network-wide HIVE generated).
CREATE OR REPLACE FUNCTION btracker_backend.get_vesting_op_stat(
    _op_name TEXT,
    _body    JSONB,
    _is_hf01 BOOLEAN
)
RETURNS SETOF btracker_backend.vesting_op_stat_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  _pre_hf1 INT := btracker_backend.vests_precision_multiplier(_is_hf01);
BEGIN
  IF _op_name = 'transfer_to_vesting_operation' THEN
    RETURN QUERY
      SELECT
        btracker_backend.vesting_kind_power_up(),
        ((_body -> 'amount' ->> 'amount')::BIGINT),
        0::NUMERIC;

  ELSIF _op_name = 'withdraw_vesting_operation' THEN
    RETURN QUERY
      SELECT
        btracker_backend.vesting_kind_power_down_init(),
        0::BIGINT,
        ((_body -> 'vesting_shares' ->> 'amount')::NUMERIC * _pre_hf1)
      WHERE (_body -> 'vesting_shares' ->> 'amount')::BIGINT <> 0;

  ELSIF _op_name = 'fill_vesting_withdraw_operation' THEN
    RETURN QUERY
      SELECT
        btracker_backend.vesting_kind_power_down_fill(),
        (CASE WHEN (_body -> 'deposited' ->> 'precision')::INT = 3 -- HIVE
              THEN (_body -> 'deposited' ->> 'amount')::BIGINT ELSE 0::BIGINT END),
        ((_body -> 'withdrawn' ->> 'amount')::NUMERIC * _pre_hf1);
  END IF;
END
$$;

RESET ROLE;
