/*
Operation parsers for HBD<->HIVE conversion operations.
Called by: db/process_block_range_converts.sql

Extracts data from operation JSONBs for:
- convert: Initiate HBD->HIVE conversion (3.5-day delay)
- collateralized_convert: Initiate HIVE->HBD conversion (3.5-day delay)
- fill_convert_request: Complete conversion after delay

Return type includes owner, request_id, input asset NAI, and amount.
The convert_state table tracks pending conversions (created - filled = remaining).
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.convert_event CASCADE;
CREATE TYPE btracker_backend.convert_event AS
(
    owner      TEXT,
    request_id BIGINT,
    nai        SMALLINT,  -- input asset
    amount_in  BIGINT     -- satoshis
);

-- Extract convert/collateralized_convert request data.
-- Creates pending conversion (3.5-day delay before fill).
-- Amount field contains the input asset (HBD for convert, HIVE for collateralized).
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
    _body->'value'->>'owner',      -- account converting
    _body->'value'->>'requestid',  -- unique request identifier
    _asset.asset_symbol_nai,       -- input asset (HBD or HIVE)
    _asset.amount                  -- amount being converted
  );

  RETURN _result;
END;
$$;

-- Extract fill_convert_request data (virtual op).
-- Completes conversion after 3.5-day delay.
-- Amount_in field contains what was converted (matches original request).
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
    _body->'value'->>'owner',      -- account receiving conversion
    _body->'value'->>'requestid',  -- matches original request
    _asset.asset_symbol_nai,       -- input asset that was converted
    _asset.amount                  -- amount filled (subtract from pending)
  );

  RETURN _result;
END;
$$;

RESET ROLE;
