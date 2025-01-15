SET ROLE btracker_owner;

DROP TYPE IF EXISTS impacted_savings_return CASCADE;
CREATE TYPE impacted_savings_return AS
(
    account_name VARCHAR,
    amount BIGINT,
    asset_symbol_nai INT,
    savings_withdraw_request INT,
    request_id BIGINT
);


CREATE OR REPLACE FUNCTION get_impacted_saving_balances(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS impacted_savings_return
LANGUAGE plpgsql
VOLATILE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 32 THEN
        process_transfer_to_savings_operation(_operation_body)

      WHEN _op_type_id = 33 THEN
        process_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 34 THEN
        process_cancel_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 59 THEN
        process_fill_transfer_from_savings_operation(_operation_body)

      WHEN _op_type_id = 55 THEN
        process_interest_operation(_operation_body)
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION process_transfer_to_savings_operation(IN _operation_body JSONB)
RETURNS impacted_savings_return
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'to')::TEXT,
    ((_operation_body)->'value'->'amount'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    0,
    NULL
  )::impacted_savings_return;
  
END
$$;

CREATE OR REPLACE FUNCTION process_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS impacted_savings_return
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    - ((_operation_body)->'value'->'amount'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION process_fill_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS impacted_savings_return
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    0,
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    -1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION process_interest_operation(IN _operation_body JSONB)
RETURNS impacted_savings_return
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'owner')::TEXT,
    ((_operation_body)->'value'->'interest'->>'amount')::BIGINT,
    substring((_operation_body)->'value'->'interest'->>'nai', '[0-9]+')::INT,
    0,
    NULL
  )::impacted_savings_return;

END
$$;

CREATE OR REPLACE FUNCTION process_cancel_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS impacted_savings_return
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    NULL,
    NULL,
    -1,
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::impacted_savings_return;

END
$$;

RESET ROLE;
