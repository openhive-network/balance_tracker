/*
Operation parsers for internal market order operations.
Called by: db/process_block_range_orders.sql

Extracts data from operation JSONBs for:
- limit_order_create/create2: Create market order (amount to sell)
- limit_order_cancel/cancelled: Cancel open order
- fill_order: Partial/full order fill (returns both sides of trade)

Return type includes owner, order_id, sold asset NAI, and amount.
fill_order returns SETOF with 0-2 rows (one per side: open_owner and current_owner).
Cancel operations return NULL for nai/amount (full order removal).
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.limit_order_event CASCADE;
CREATE TYPE btracker_backend.limit_order_event AS
(
    owner    TEXT,
    order_id BIGINT,
    nai      SMALLINT, -- sold asset (HIVE/HBD)
    amount   BIGINT    -- satoshis
);

-- Extract limit_order_create/create2 data.
-- Creates open order on internal market. Amount_to_sell is locked until filled/cancelled.
CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_create_event(IN _body JSONB)
RETURNS btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _asset btracker_backend.asset;
BEGIN
  _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'amount_to_sell');

  RETURN (
    _body -> 'value' ->> 'owner',    -- order creator
    _body -> 'value' ->> 'orderid',  -- user-specified order ID
    _asset.asset_symbol_nai,         -- asset being sold (HIVE or HBD)
    _asset.amount                    -- amount locked in order
  )::btracker_backend.limit_order_event;
END;
$$;

-- Extract limit_order_cancel/cancelled data.
-- Removes open order. NAI/amount are NULL (full order removal).
-- Handles both user cancel (owner field) and system cancel (seller field).
CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_cancel_event(IN _body JSONB)
RETURNS btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
BEGIN
  RETURN (
    COALESCE(_body -> 'value' ->> 'seller', _body -> 'value' ->> 'owner'),  -- order owner
    _body -> 'value' ->> 'orderid',  -- order to cancel
    NULL,                             -- NAI not needed (full removal)
    NULL                              -- amount not needed (full removal)
  )::btracker_backend.limit_order_event;
END;
$$;

-- Extract fill_order data (virtual op).
-- Order match affects TWO orders. Returns 0-2 rows (one per side).
-- Each side's amount is subtracted from their open order balance.
CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_fill_events(IN _body JSONB)
RETURNS SETOF btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _asset btracker_backend.asset;
BEGIN
  -- Open order side (existing order in the book)
  IF _body -> 'value' ->> 'open_owner' IS NOT NULL THEN
    _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'open_pays');

    RETURN NEXT (
      _body -> 'value' ->> 'open_owner',
      _body -> 'value' ->> 'open_orderid',
      _asset.asset_symbol_nai,
      _asset.amount             -- amount filled from this order
    )::btracker_backend.limit_order_event;
  END IF;

  -- Current order side (taker order that matched)
  IF _body -> 'value' ->> 'current_owner' IS NOT NULL THEN
    _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'current_pays');

    RETURN NEXT (
      _body -> 'value' ->> 'current_owner',
      _body -> 'value' ->> 'current_orderid',
      _asset.asset_symbol_nai,
      _asset.amount             -- amount filled from this order
    )::btracker_backend.limit_order_event;
  END IF;

  RETURN;
END;
$$;

RESET ROLE;
