SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.limit_order_event CASCADE;
CREATE TYPE btracker_backend.limit_order_event AS
(
    owner    TEXT,
    order_id BIGINT,
    nai      SMALLINT, -- sold asset (HIVE/HBD)
    amount   BIGINT    -- satoshis
);

-- Process limit_order_create_operation and limit_order_create2_operation (returns single row)
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
    _body -> 'value' ->> 'owner',
    _body -> 'value' ->> 'orderid',
    _asset.asset_symbol_nai,
    _asset.amount
  )::btracker_backend.limit_order_event;
END;
$$;

-- Process limit_order_cancel_operation and limit_order_cancelled_operation (returns single row)
CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_cancel_event(IN _body JSONB)
RETURNS btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
BEGIN
  RETURN (
    COALESCE(_body -> 'value' ->> 'seller', _body -> 'value' ->> 'owner'),
    _body -> 'value' ->> 'orderid',
    NULL,
    NULL
  )::btracker_backend.limit_order_event;
END;
$$;

-- Process fill_order_operation (returns SETOF with 0-2 rows)
CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_fill_events(IN _body JSONB)
RETURNS SETOF btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _asset btracker_backend.asset;
BEGIN
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

  RETURN;
END;
$$;

RESET ROLE;
