SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.convert_event CASCADE;
CREATE TYPE btracker_backend.convert_event AS
(
    owner      TEXT,
    request_id BIGINT,
    nai        SMALLINT,  -- input asset
    amount_in  BIGINT     -- satoshis
);

-- Process convert_operation (uses 'amount' field)
CREATE OR REPLACE FUNCTION btracker_backend.get_convert_request_event(IN _body JSONB)
RETURNS btracker_backend.convert_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _asset  btracker_backend.asset;
  _result btracker_backend.convert_event;
BEGIN
  _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'amount');

  _result := (
    _body->'value'->>'owner',
    _body->'value'->>'requestid',
    _asset.asset_symbol_nai,
    _asset.amount
  );

  RETURN _result;
END;
$$;

-- Process fill_convert_request_operation (uses 'amount_in' field)
CREATE OR REPLACE FUNCTION btracker_backend.get_fill_convert_event(IN _body JSONB)
RETURNS btracker_backend.convert_event
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  _asset  btracker_backend.asset;
  _result btracker_backend.convert_event;
BEGIN
  _asset := btracker_backend.parse_amount_object(_body -> 'value' -> 'amount_in');

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
