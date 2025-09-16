SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.convert_event CASCADE;
CREATE TYPE btracker_backend.convert_event AS
(
    owner      TEXT,
    request_id BIGINT,
    nai        SMALLINT,  -- input asset
    amount_in  BIGINT,    -- satoshis
    kind       TEXT       -- 'create' | 'fill'
);

-- the SRF parser used by process_block_range_converts
DROP FUNCTION IF EXISTS btracker_backend.get_convert_events(jsonb,int);
CREATE OR REPLACE FUNCTION btracker_backend.get_convert_events(
    IN _body jsonb, IN _op_type_id int
) RETURNS SETOF btracker_backend.convert_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _CONVERT CONSTANT int := 8;   -- hive::protocol::convert_operation
  _FILL    CONSTANT int := 50;  -- hive::protocol::fill_convert_request_operation

  _owner      text;
  _request_id bigint;
  _nai        smallint;
  _amt        bigint;
BEGIN
  IF _op_type_id = _CONVERT THEN
    _owner      := (_body->'value'->>'owner')::text;
    _request_id := (_body->'value'->>'requestid')::bigint;
    _nai        := substring((_body->'value'->'amount'->>'nai') FROM 3)::smallint;
    _amt        := (_body->'value'->'amount'->>'amount')::bigint;

    RETURN QUERY SELECT _owner, _request_id, _nai, _amt, 'create'::text;

  ELSIF _op_type_id = _FILL THEN
    _owner      := (_body->'value'->>'owner')::text;
    _request_id := (_body->'value'->>'requestid')::bigint;
    _nai        := substring((_body->'value'->'amount_in'->>'nai') FROM 3)::smallint;
    _amt        := (_body->'value'->'amount_in'->>'amount')::bigint;

    RETURN QUERY SELECT _owner, _request_id, _nai, _amt, 'fill'::text;
  END IF;

  RETURN;
END;
$$;

RESET ROLE;
