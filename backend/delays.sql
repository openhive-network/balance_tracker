SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_delays_return CASCADE;
CREATE TYPE btracker_backend.impacted_delays_return AS
(
    from_account VARCHAR,
    withdrawn BIGINT,
    to_account VARCHAR,
    deposited BIGINT
);

-- Process fill_vesting_withdraw_operation (for delays)
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_vesting_withdraw_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  IF ((_operation_body)->'value'->'deposited'->>'precision')::INT = 6 THEN
    RETURN (
      ((_operation_body)->'value'->>'from_account')::TEXT,
      - ((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT,
      ((_operation_body)->'value'->>'to_account')::TEXT,
      ((_operation_body)->'value'->'deposited'->>'amount')::BIGINT
    )::btracker_backend.impacted_delays_return;
  ELSE
    RETURN (
      ((_operation_body)->'value'->>'from_account')::TEXT,
      - ((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT,
      NULL,
      NULL
    )::btracker_backend.impacted_delays_return;
  END IF;
END
$$;

-- Process transfer_to_vesting_completed_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_to_vesting_completed_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'to_account')::TEXT,
    ((_operation_body)->'value'->'vesting_shares_received'->>'amount')::BIGINT,
    NULL,
    NULL
  )::btracker_backend.impacted_delays_return;
END
$$;

-- Process delayed_voting_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_delayed_voting_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'voter')::TEXT,
    - ((_operation_body)->'value'->'votes')::BIGINT,
    NULL,
    NULL
  )::btracker_backend.impacted_delays_return;
END
$$;

RESET ROLE;
