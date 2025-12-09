SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.asset CASCADE;
CREATE TYPE btracker_backend.asset AS
(
    amount           BIGINT, -- Amount of asset
    asset_precision  INT,    -- Precision of assets
    asset_symbol_nai INT     -- Type of asset symbol used in the operation
);

CREATE OR REPLACE FUNCTION btracker_backend.parse_amount_object(__amount_object JSONB)
RETURNS btracker_backend.asset IMMUTABLE
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __nai        hafd.asset_symbol;
  __asset_info hive.asset_symbol_info;

  __result     btracker_backend.asset;
BEGIN
  __nai := hive.asset_symbol_from_nai_string(
    (__amount_object ->> 'nai'),
    (__amount_object ->> 'precision')::SMALLINT
  );

  __asset_info := hive.decode_asset_symbol(__nai);

  __result := (
    __amount_object->>'amount',
    __asset_info.precision,
    __asset_info.nai
  );

  RETURN __result;
END
$$;

RESET ROLE;
