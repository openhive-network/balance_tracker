SET ROLE btracker_owner;

DROP TYPE IF EXISTS impacted_delays_return CASCADE;
CREATE TYPE impacted_delays_return AS
(
    from_account VARCHAR,
    withdrawn BIGINT,
    to_account VARCHAR,
    deposited BIGINT
);

CREATE OR REPLACE FUNCTION get_impacted_delayed_balances(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS impacted_delays_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 56 THEN
        process_fill_vesting_withdraw_operation(_operation_body)

      WHEN _op_type_id = 77 THEN
        process_transfer_to_vesting_completed_operation(_operation_body)

      WHEN _op_type_id = 70 THEN
        process_delayed_voting_operation(_operation_body) 
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION process_fill_vesting_withdraw_operation(IN _operation_body JSONB)
RETURNS impacted_delays_return
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
    )::impacted_delays_return;
  ELSE
    RETURN (
      ((_operation_body)->'value'->>'from_account')::TEXT,
      - ((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT,
      NULL,
      NULL
    )::impacted_delays_return;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION process_transfer_to_vesting_completed_operation(IN _operation_body JSONB)
RETURNS impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'to_account')::TEXT,
    ((_operation_body)->'value'->'vesting_shares_received'->>'amount')::BIGINT,
    NULL,
    NULL
  )::impacted_delays_return;
END
$$;

CREATE OR REPLACE FUNCTION process_delayed_voting_operation(IN _operation_body JSONB)
RETURNS impacted_delays_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'voter')::TEXT,
    - ((_operation_body)->'value'->'votes')::BIGINT,
    NULL,
    NULL
  )::impacted_delays_return;
END
$$;

RESET ROLE;
