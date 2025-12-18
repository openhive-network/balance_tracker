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

-- Process withdraw_vesting_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_withdraw_vesting_operation(IN _operation_body JSONB, IN _is_hf01 BOOLEAN, IN _is_hf16 BOOLEAN)
RETURNS btracker_backend.impacted_withdraws_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _pre_hf1  INT := btracker_backend.vests_precision_multiplier(_is_hf01);
  _rate     INT := btracker_backend.withdrawal_rate_weeks(_is_hf16);
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

-- Process fill_vesting_withdraw_operation (for withdrawals tracking)
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
    (((_operation_body)->'value'->'withdrawn'->>'amount')::BIGINT * _pre_hf1)
  )::btracker_backend.impacted_fill_withdraw_return;

END
$$;

-- Process set_withdraw_vesting_route_operation
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

-- Process hardfork_hive_operation (HF23 withdrawal reset)
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
