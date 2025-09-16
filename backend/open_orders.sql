SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.limit_order_event CASCADE;
CREATE TYPE btracker_backend.limit_order_event AS
(
    owner    TEXT,
    order_id BIGINT,
    nai      SMALLINT,  -- sold asset (HIVE/HBD)
    amount   BIGINT,    -- satoshis
    kind     TEXT,      -- 'create' | 'fill' | 'cancel'
    side     TEXT       -- NULL | 'open' | 'current'  (only for fills)
);


CREATE OR REPLACE FUNCTION btracker_backend.get_limit_order_events(
    IN _body jsonb, IN _op_type_id int
) RETURNS SETOF btracker_backend.limit_order_event
LANGUAGE plpgsql STABLE AS
$$
DECLARE
  -- hardcoded ids
  _CREATE1   CONSTANT int := 5;
  _CREATE2   CONSTANT int := 21;
  _FILL      CONSTANT int := 57;
  _CANCEL    CONSTANT int := 6;
  _CANCELLED CONSTANT int := 85;

  _owner           text;
  _order_id        bigint;
  _nai             smallint;
  _amt             bigint;

  _open_owner      text;
  _current_owner   text;
  _open_orderid    bigint;
  _current_orderid bigint;
  _open_nai        smallint;
  _current_nai     smallint;
  _open_amt        bigint;
  _current_amt     bigint;
BEGIN
  IF _op_type_id = _CREATE1 OR _op_type_id = _CREATE2 THEN
    _owner    := (_body->'value'->>'owner')::text;
    _order_id := (_body->'value'->>'orderid')::bigint;
    _nai      := substring(_body->'value'->'amount_to_sell'->>'nai' FROM 3)::smallint;
    _amt      := (_body->'value'->'amount_to_sell'->>'amount')::bigint;

    RETURN QUERY SELECT _owner, _order_id, _nai, _amt, 'create'::text, NULL::text;

  ELSIF _op_type_id = _CANCEL OR _op_type_id = _CANCELLED THEN
    _owner    := COALESCE((_body->'value'->>'seller')::text, (_body->'value'->>'owner')::text);
    _order_id := (_body->'value'->>'orderid')::bigint;

    RETURN QUERY SELECT _owner, _order_id, NULL::smallint, NULL::bigint, 'cancel'::text, NULL::text;

  ELSIF _op_type_id = _FILL THEN
    _open_owner      := (_body->'value'->>'open_owner')::text;
    _current_owner   := (_body->'value'->>'current_owner')::text;
    _open_orderid    := (_body->'value'->>'open_orderid')::bigint;
    _current_orderid := (_body->'value'->>'current_orderid')::bigint;

    _open_nai        := substring((_body->'value'->'open_pays'->>'nai') FROM 3)::smallint;
    _current_nai     := substring((_body->'value'->'current_pays'->>'nai') FROM 3)::smallint;
    _open_amt        := (_body->'value'->'open_pays'->>'amount')::bigint;
    _current_amt     := (_body->'value'->'current_pays'->>'amount')::bigint;

    IF _open_owner IS NOT NULL THEN
      RETURN QUERY SELECT _open_owner, _open_orderid, _open_nai, _open_amt, 'fill'::text, 'open'::text;
    END IF;
    IF _current_owner IS NOT NULL THEN
      RETURN QUERY SELECT _current_owner, _current_orderid, _current_nai, _current_amt, 'fill'::text, 'current'::text;
    END IF;
  END IF;

  RETURN;
END;
$$;

RESET ROLE;
