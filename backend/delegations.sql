SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_delegations_return CASCADE;
CREATE TYPE btracker_backend.impacted_delegations_return AS
(
    delegator VARCHAR,
    delegatee VARCHAR,
    amount BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_delegation_balances(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 40 THEN
        btracker_backend.process_delegate_vesting_shares_operation(_operation_body)

      WHEN _op_type_id = 41 THEN
        btracker_backend.process_account_create_with_delegation_operation(_operation_body)

      WHEN _op_type_id = 62 THEN
        btracker_backend.process_return_vesting_shares_operation(_operation_body)

      WHEN _op_type_id = 68 THEN
        btracker_backend.process_hf23_acccounts(_operation_body)
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.process_delegate_vesting_shares_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'delegator')::TEXT,
    ((_operation_body)->'value'->>'delegatee')::TEXT,
    ((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT
  )::btracker_backend.impacted_delegations_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_account_create_with_delegation_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'creator')::TEXT,
    ((_operation_body)->'value'->>'new_account_name')::TEXT,
    ((_operation_body)->'value'->'delegation'->>'amount')::BIGINT
  )::btracker_backend.impacted_delegations_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_return_vesting_shares_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    NULL,
    - ((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT
  )::btracker_backend.impacted_delegations_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_hf23_acccounts(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    NULL,
    0
  )::btracker_backend.impacted_delegations_return;
  
END
$$;

RESET ROLE;
