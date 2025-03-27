SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_withdraws_return CASCADE;
CREATE TYPE btracker_backend.impacted_withdraws_return AS
(
    account_name VARCHAR,
    withdrawn BIGINT,
    vesting_withdraw_rate BIGINT,
    to_withdraw BIGINT
);

DROP TYPE IF EXISTS btracker_backend.impacted_fill_withdraw_return CASCADE;
CREATE TYPE btracker_backend.impacted_fill_withdraw_return AS
(
    account_name VARCHAR,
    withdrawn BIGINT
);

DROP TYPE IF EXISTS btracker_backend.impacted_withdraw_routes_return CASCADE;
CREATE TYPE btracker_backend.impacted_withdraw_routes_return AS
(
    from_account VARCHAR,
    to_account VARCHAR,
    percent BIGINT
);


CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_withdraws(IN _operation_body JSONB, IN _op_type_id INT, IN _source_op_block INT)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 4 THEN
        btracker_backend.process_withdraw_vesting_operation(
          _operation_body,
          (SELECT (block_num < _source_op_block) FROM hafd.applied_hardforks WHERE hardfork_num = 1),
          (SELECT (block_num < _source_op_block) FROM hafd.applied_hardforks WHERE hardfork_num = 16)
        )

      WHEN _op_type_id = 68 THEN
        btracker_backend.process_reset_withdraw_hf23(_operation_body)
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.process_withdraw_vesting_operation(IN _operation_body JSONB, IN _is_hf01 BOOLEAN, IN _is_hf16 BOOLEAN)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _pre_hf1 INT := (CASE WHEN _is_hf01 THEN 1 ELSE 1000000 END);
  _rate INT := (CASE WHEN _is_hf16 THEN 13 ELSE 104 END);
  _withdraw BIGINT := GREATEST(((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT, 0);
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    0, -- resets withdrawn
    ((_withdraw * _pre_hf1) / _rate)::BIGINT,
    (_withdraw * _pre_hf1)::BIGINT
  )::btracker_backend.impacted_withdraws_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_fill_vesting_withdraw_operation(IN _operation_body JSONB, IN _is_hf01 BOOLEAN)
RETURNS btracker_backend.impacted_fill_withdraw_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _pre_hf1 INT := (CASE WHEN _is_hf01 THEN 1 ELSE 1000000 END);
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from_account')::TEXT,
    (((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT * _pre_hf1)
  )::btracker_backend.impacted_fill_withdraw_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_set_withdraw_vesting_route_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_withdraw_routes_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from_account')::TEXT,
    ((_operation_body)->'value'->>'to_account')::TEXT,
    ((_operation_body)->'value'->>'percent')::INT
  )::btracker_backend.impacted_withdraw_routes_return;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_reset_withdraw_hf23(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,
    0, -- resets withdrawn
    0,
    0
  )::btracker_backend.impacted_withdraws_return;
  
END
$$;

RESET ROLE;
