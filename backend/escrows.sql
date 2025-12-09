SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.escrow_transfer_type CASCADE;
CREATE TYPE btracker_backend.escrow_transfer_type AS
(
    from_name   TEXT,
    escrow_id   BIGINT,
    hive_nai    SMALLINT,
    hive_amount BIGINT,
    hbd_nai     SMALLINT,
    hbd_amount  BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_transfers(IN __body JSONB)
RETURNS btracker_backend.escrow_transfer_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  -- Parse amounts for transfer and release operations
  __hive_amt btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hive_amount');
  __hbd_amt  btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hbd_amount' );

  __result   btracker_backend.escrow_transfer_type;
BEGIN

  __result := (
    __body -> 'value' ->> 'from',
    __body -> 'value' ->> 'escrow_id',
    __hive_amt.asset_symbol_nai,
    __hive_amt.amount,
    __hbd_amt.asset_symbol_nai,
    __hbd_amt.amount
  );

  RETURN __result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_release(IN __body JSONB)
RETURNS btracker_backend.escrow_transfer_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  -- Parse amounts for transfer and release operations
  __hive_amt btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hive_amount');
  __hbd_amt  btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hbd_amount' );

  __result   btracker_backend.escrow_transfer_type;
BEGIN

  __result := (
    __body -> 'value' ->> 'from',
    __body -> 'value' ->> 'escrow_id',
    __hive_amt.asset_symbol_nai,
    - __hive_amt.amount,               -- for release operations, amounts are negative
    __hbd_amt.asset_symbol_nai,
    - __hbd_amt.amount
  );

  RETURN __result;
END;
$BODY$;


DROP TYPE IF EXISTS btracker_backend.escrow_fee_type CASCADE;
CREATE TYPE btracker_backend.escrow_fee_type AS
(
    from_name   TEXT,
    escrow_id   BIGINT,
    nai         SMALLINT,
    amount      BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_fees(IN __body JSONB)
RETURNS btracker_backend.escrow_fee_type
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  __op_approved INT := 89; -- hive::protocol::escrow_approved_operation
  __fee    btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'fee');
  __result btracker_backend.escrow_fee_type;
BEGIN
  -- only transfer and approved operations have fees
  __result := (
    __body -> 'value' ->> 'from',
    __body -> 'value' ->> 'escrow_id',
    __fee.asset_symbol_nai,
    __fee.amount
  );

  RETURN __result;
END;
$$;

DROP TYPE IF EXISTS btracker_backend.escrow_rejects_and_approved_type CASCADE;
CREATE TYPE btracker_backend.escrow_rejects_and_approved_type AS
(
    from_name   TEXT,
    escrow_id   BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_rejects(IN __body JSONB)
RETURNS btracker_backend.escrow_rejects_and_approved_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __result btracker_backend.escrow_rejects_and_approved_type;
BEGIN
  -- when rejected the entire escrow is cancelled
  __result := (
    __body -> 'value' ->> 'from',
    __body -> 'value' ->> 'escrow_id'
  );

  RETURN __result;
END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_approves(IN __body JSONB)
RETURNS btracker_backend.escrow_rejects_and_approved_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __result btracker_backend.escrow_rejects_and_approved_type;
BEGIN
  -- when rejected the entire escrow is cancelled
  __result := (
    __body -> 'value' ->> 'from',
    __body -> 'value' ->> 'escrow_id'
  );

  RETURN __result;
END;
$BODY$;

RESET ROLE;
