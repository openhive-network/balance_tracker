/*
Operation parsers for power-down (vesting withdrawal) operations.
Called by: db/process_withdrawals.sql

Extracts data from operation JSONBs for:
- withdraw_vesting: Start/cancel power-down (sets rate and total to withdraw)
- fill_vesting_withdraw: Weekly power-down payout (tracks cumulative withdrawn)
- set_withdraw_vesting_route: Configure withdrawal routing to other accounts
- hardfork_hive: HF23 withdrawal reset for Steemit Inc accounts

Handles hardfork-specific logic:
- Pre-HF1: VESTS had different precision (multiplier applied)
- HF16: Changed power-down from 104 weeks to 13 weeks (rate calculation)

Three return types for different operation categories:
- impacted_withdraws_return: Power-down state (rate, to_withdraw, withdrawn)
- impacted_fill_withdraw_return: Fill event (account, withdrawn amount)
- impacted_withdraw_routes_return: Routing rules (from, to, percent)
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_withdraws_return CASCADE;
CREATE TYPE btracker_backend.impacted_withdraws_return AS
(
    account_name          VARCHAR,
    withdrawn             BIGINT,
    vesting_withdraw_rate BIGINT,
    to_withdraw           BIGINT
);

DROP TYPE IF EXISTS btracker_backend.impacted_fill_withdraw_return CASCADE;
CREATE TYPE btracker_backend.impacted_fill_withdraw_return AS
(
    account_name VARCHAR,
    withdrawn    BIGINT
);

DROP TYPE IF EXISTS btracker_backend.impacted_withdraw_routes_return CASCADE;
CREATE TYPE btracker_backend.impacted_withdraw_routes_return AS
(
    from_account VARCHAR,
    to_account   VARCHAR,
    percent      BIGINT
);

-- Extract withdraw_vesting (power-down) data.
-- Initiates or cancels power-down. Rate depends on hardfork (104 or 13 weeks).
CREATE OR REPLACE FUNCTION btracker_backend.process_withdraw_vesting_operation(IN _operation_body JSONB, IN _is_hf01 BOOLEAN, IN _is_hf16 BOOLEAN)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _pre_hf1  INT := btracker_backend.vests_precision_multiplier(_is_hf01);  -- pre-HF1 precision adjustment
  _rate     INT := btracker_backend.withdrawal_rate_weeks(_is_hf16);       -- 104 weeks pre-HF16, 13 after
  _withdraw BIGINT := GREATEST(((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT, 0);
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    0,                                        -- resets withdrawn counter
    ((_withdraw * _pre_hf1) / _rate)::BIGINT, -- weekly withdrawal rate
    (_withdraw * _pre_hf1)::BIGINT            -- total to withdraw
  )::btracker_backend.impacted_withdraws_return;

END
$$;

-- Extract fill_vesting_withdraw data for withdrawal tracking.
-- Weekly power-down payout. Adds to cumulative withdrawn amount.
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_vesting_withdraw_operation_for_withdrawals(IN _operation_body JSONB, IN _is_hf01 BOOLEAN)
RETURNS btracker_backend.impacted_fill_withdraw_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _pre_hf1 INT := btracker_backend.vests_precision_multiplier(_is_hf01);
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from_account')::TEXT,
    (((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT * _pre_hf1)  -- cumulative withdrawn
  )::btracker_backend.impacted_fill_withdraw_return;

END
$$;

-- Extract set_withdraw_vesting_route data.
-- Configure withdrawal routing (send power-down to another account).
-- Percent is in HIVE_1_PERCENT units (100 = 1%). 0 removes the route.
CREATE OR REPLACE FUNCTION btracker_backend.process_set_withdraw_vesting_route_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_withdraw_routes_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from_account')::TEXT,  -- power-down source
    ((_operation_body)->'value'->>'to_account')::TEXT,    -- route destination
    ((_operation_body)->'value'->>'percent')::INT         -- percentage (0-10000)
  )::btracker_backend.impacted_withdraw_routes_return;

END
$$;

-- Extract HF23 affected account for withdrawal reset.
-- Steemit Inc accounts had all power-down state zeroed at the Hive fork.
CREATE OR REPLACE FUNCTION btracker_backend.process_reset_withdraw_hf23(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    0,  -- reset withdrawn counter
    0,  -- reset withdrawal rate
    0   -- reset total to withdraw
  )::btracker_backend.impacted_withdraws_return;

END
$$;

RESET ROLE;
