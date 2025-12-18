/*
Operation parsers for escrow operations.
Called by: db/process_block_range_escrows.sql

Extracts data from operation JSONBs for:
- escrow_transfer: Create escrow with HIVE/HBD amounts and agent fee
- escrow_release: Release funds from escrow (amounts are negated)
- escrow_approve: Agent or receiver approves escrow
- escrow_reject: Agent or receiver rejects escrow (full cancellation)
- escrow_dispute: Mark escrow as disputed (agent arbitrates)

Return types:
- escrow_transfer_type: Full escrow data (from, id, hive_nai, hive_amt, hbd_nai, hbd_amt)
- escrow_fee_type: Agent fee data (from, id, nai, amount)
- escrow_rejects_and_approved_type: Simple (from, id) for state changes

Release operations negate amounts to subtract from escrow balance.
Rejection deletes the entire escrow and its pending fees.
*/

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

-- Extract escrow_transfer data.
-- Creates new escrow with HIVE and/or HBD amounts locked until release/dispute.
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_transfers(IN __body JSONB)
RETURNS btracker_backend.escrow_transfer_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __hive_amt btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hive_amount');
  __hbd_amt  btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hbd_amount' );

  __result   btracker_backend.escrow_transfer_type;
BEGIN

  __result := (
    __body -> 'value' ->> 'from',     -- escrow creator
    __body -> 'value' ->> 'escrow_id', -- unique ID per account
    __hive_amt.asset_symbol_nai,
    __hive_amt.amount,                 -- HIVE locked in escrow
    __hbd_amt.asset_symbol_nai,
    __hbd_amt.amount                   -- HBD locked in escrow
  );

  RETURN __result;
END;
$BODY$;

-- Extract escrow_release data.
-- Releases funds from escrow to recipient. Amounts are negated to subtract from escrow.
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_release(IN __body JSONB)
RETURNS btracker_backend.escrow_transfer_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __hive_amt btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hive_amount');
  __hbd_amt  btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'hbd_amount' );

  __result   btracker_backend.escrow_transfer_type;
BEGIN

  __result := (
    __body -> 'value' ->> 'from',     -- escrow owner
    __body -> 'value' ->> 'escrow_id',
    __hive_amt.asset_symbol_nai,
    - __hive_amt.amount,               -- negative: reduces escrow HIVE balance
    __hbd_amt.asset_symbol_nai,
    - __hbd_amt.amount                 -- negative: reduces escrow HBD balance
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

-- Extract escrow fee data.
-- Agent fee specified at escrow creation. Paid on approval, refunded on rejection.
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_fees(IN __body JSONB)
RETURNS btracker_backend.escrow_fee_type
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  __fee    btracker_backend.asset := btracker_backend.parse_amount_object(__body -> 'value' -> 'fee');
  __result btracker_backend.escrow_fee_type;
BEGIN
  __result := (
    __body -> 'value' ->> 'from',     -- escrow owner
    __body -> 'value' ->> 'escrow_id',
    __fee.asset_symbol_nai,           -- fee asset type
    __fee.amount                      -- fee amount for agent
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

-- Extract escrow_rejected data.
-- Rejection cancels entire escrow. Funds returned to creator, fee refunded.
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_rejects(IN __body JSONB)
RETURNS btracker_backend.escrow_rejects_and_approved_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __result btracker_backend.escrow_rejects_and_approved_type;
BEGIN
  __result := (
    __body -> 'value' ->> 'from',     -- escrow owner
    __body -> 'value' ->> 'escrow_id' -- escrow to delete
  );

  RETURN __result;
END;
$BODY$;

-- Extract escrow_approve data.
-- Approval by agent or receiver. Both must approve before funds can be released.
-- Fee is paid to agent on approval.
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_approves(IN __body JSONB)
RETURNS btracker_backend.escrow_rejects_and_approved_type
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __result btracker_backend.escrow_rejects_and_approved_type;
BEGIN
  __result := (
    __body -> 'value' ->> 'from',     -- escrow owner
    __body -> 'value' ->> 'escrow_id' -- escrow being approved
  );

  RETURN __result;
END;
$BODY$;

RESET ROLE;
