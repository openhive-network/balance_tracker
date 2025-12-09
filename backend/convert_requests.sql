SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.convert_event CASCADE;
CREATE TYPE btracker_backend.convert_event AS
(
    owner      TEXT,
    request_id BIGINT,
    nai        SMALLINT,  -- input asset
    amount_in  BIGINT     -- satoshis
);

-- the SRF parser used by process_block_range_converts
DROP FUNCTION IF EXISTS btracker_backend.get_convert_events;
CREATE OR REPLACE FUNCTION btracker_backend.get_convert_events(
    IN _body jsonb,
    IN _op_type_id INT
)
RETURNS btracker_backend.convert_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _CONVERT CONSTANT INT := 8;   -- hive::protocol::convert_operation
  _FILL    CONSTANT INT := 50;  -- hive::protocol::fill_convert_request_operation
  _asset   btracker_backend.asset;
  _result  btracker_backend.convert_event;

  _asset_field_name TEXT := (CASE WHEN _op_type_id = _FILL THEN 'amount_in' ELSE 'amount' END);
BEGIN
  _asset := btracker_backend.parse_amount_object(_body -> 'value' -> _asset_field_name);

  _result := (
    _body->'value'->>'owner',
    _body->'value'->>'requestid',
    _asset.asset_symbol_nai,
    _asset.amount
  );

  RETURN _result;
END;
$$;

RESET ROLE;
