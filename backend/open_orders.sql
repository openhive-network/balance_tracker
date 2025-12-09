SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.limit_order_event CASCADE;
CREATE TYPE btracker_backend.limit_order_event AS
(
    owner    TEXT,
    order_id BIGINT,
    nai      SMALLINT, -- sold asset (HIVE/HBD)
    amount   BIGINT    -- satoshis
    --kind     TEXT,   -- 'create' | 'fill' | 'cancel'                -- not needed
    --side     TEXT    -- NULL | 'open' | 'current'  (only for fills) -- unused
);

CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_events(
    IN _body JSONB,
    IN _op_type_id INT
)
RETURNS SETOF btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  -- hardcoded ids
  _CREATE1   CONSTANT INT := 5;  -- hive::protocol::limit_order_create_operation
  _CREATE2   CONSTANT INT := 21; -- hive::protocol::limit_order_create2_operation
  _FILL      CONSTANT INT := 57; -- hive::protocol::fill_order_operation
  _CANCEL    CONSTANT INT := 6;  -- hive::protocol::limit_order_cancel_operation
  _CANCELLED CONSTANT INT := 85; -- hive::protocol::limit_order_cancelled_operation

  _asset     btracker_backend.asset;
BEGIN
  IF _op_type_id = _CREATE1 OR _op_type_id = _CREATE2 THEN

    _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'amount_to_sell');

    RETURN NEXT (
      _body -> 'value' ->> 'owner',
      _body -> 'value' ->> 'orderid',
      _asset.asset_symbol_nai,
      _asset.amount
    )::btracker_backend.limit_order_event;

  ELSIF _op_type_id = _CANCEL OR _op_type_id = _CANCELLED THEN

    RETURN NEXT (
      COALESCE(_body -> 'value' ->> 'seller', _body -> 'value' ->> 'owner'),
      _body -> 'value' ->> 'orderid',
      NULL,
      NULL
    )::btracker_backend.limit_order_event;

  ELSIF _op_type_id = _FILL THEN

    -- Return fill event for open_owner side
    IF _body -> 'value' ->> 'open_owner' IS NOT NULL THEN
      _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'open_pays');

      RETURN NEXT (
        _body -> 'value' ->> 'open_owner',
        _body -> 'value' ->> 'open_orderid',
        _asset.asset_symbol_nai,
        _asset.amount
      )::btracker_backend.limit_order_event;
    END IF;

    -- Return fill event for current_owner side
    IF _body -> 'value' ->> 'current_owner' IS NOT NULL THEN
      _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'current_pays');

      RETURN NEXT (
        _body -> 'value' ->> 'current_owner',
        _body -> 'value' ->> 'current_orderid',
        _asset.asset_symbol_nai,
        _asset.amount
      )::btracker_backend.limit_order_event;
    END IF;

  END IF;

  RETURN;
END;
$$;

RESET ROLE;
