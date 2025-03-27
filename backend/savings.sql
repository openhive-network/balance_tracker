SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_savings_return CASCADE;
CREATE TYPE btracker_backend.impacted_savings_return AS
(
    account_name VARCHAR,
    amount BIGINT,
    asset_symbol_nai INT,
    savings_withdraw_request INT,
    request_id BIGINT
);


CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_saving_balances(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 32 THEN
        btracker_backend.process_transfer_to_savings_operation(_operation_body)

      WHEN _op_type_id = 33 THEN
        btracker_backend.process_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 34 THEN
        btracker_backend.process_cancel_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 59 THEN
        btracker_backend.process_fill_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 55 THEN
        btracker_backend.process_interest_operation(_operation_body)
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_to_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'to')::TEXT,
    ((_operation_body)->'value'->'amount'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    0,
    NULL
  )::btracker_backend.impacted_savings_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    - ((_operation_body)->'value'->'amount'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_fill_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    0,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    -1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_interest_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'owner')::TEXT,
    ((_operation_body)->'value'->'interest'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'interest'->>'nai', '[0-9]+')::INT,
    0,
    NULL
  )::btracker_backend.impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_cancel_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    NULL,
    NULL,
    -1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

RESET ROLE;
