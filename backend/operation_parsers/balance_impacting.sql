/*
Operation parsers for withdrawal delay operations.
Called by: db/process_withdrawals.sql

Extracts data from operation JSONBs for:
- fill_vesting_withdraw: Completed power-down payout (withdrawn/deposited amounts)
- transfer_to_vesting_completed: Power-up completion (vesting shares received)
- delayed_voting: HF24 delayed voting mechanism (30-day delay before full voting power)

Return type includes from_account, withdrawn amount, to_account, and deposited amount.
The withdrawn field is negative (reducing balance) for fill and delayed_voting operations.
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_delays_return CASCADE;
CREATE TYPE btracker_backend.impacted_delays_return AS
(
    from_account VARCHAR,
    withdrawn BIGINT,
    to_account VARCHAR,
    deposited BIGINT
);

-- Extract fill_vesting_withdraw data for delayed_vests tracking.
-- Precision 6 = VESTS deposited to another account (routed withdrawal).
-- Otherwise, withdrawn amount only (self-withdrawal as HIVE).
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_vesting_withdraw_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  -- Check if deposited asset is VESTS (precision 6) indicating routed withdrawal
  IF ((_operation_body)->'value'->'deposited'->>'precision')::INT = 6 THEN
    RETURN (
      ((_operation_body)->'value'->>'from_account')::TEXT,
      - ((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT,  -- negative: reduces delayed_vests
      ((_operation_body)->'value'->>'to_account')::TEXT,
      ((_operation_body)->'value'->'deposited'->>'amount')::BIGINT     -- positive: adds to recipient
    )::btracker_backend.impacted_delays_return;
  ELSE
    -- Non-VESTS deposit (HIVE): only track withdrawn amount
    RETURN (
      ((_operation_body)->'value'->>'from_account')::TEXT,
      - ((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT,
      NULL,
      NULL
    )::btracker_backend.impacted_delays_return;
  END IF;
END
$$;

-- Extract transfer_to_vesting_completed data.
-- Adds to delayed_vests when power-up completes (HF24+ delayed voting).
CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_to_vesting_completed_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'to_account')::TEXT,
    ((_operation_body)->'value'->'vesting_shares_received'->>'amount')::BIGINT,  -- positive: adds delayed_vests
    NULL,
    NULL
  )::btracker_backend.impacted_delays_return;
END
$$;

-- Extract delayed_voting data.
-- Reduces delayed_vests as voting power matures (30-day unlock period).
CREATE OR REPLACE FUNCTION btracker_backend.process_delayed_voting_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'voter')::TEXT,
    - ((_operation_body)->'value'->'votes')::BIGINT,  -- negative: reduces delayed_vests as votes mature
    NULL,
    NULL
  )::btracker_backend.impacted_delays_return;
END
$$;

RESET ROLE;
