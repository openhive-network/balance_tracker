SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_nai_type(_nai btracker_backend.nai_type)
RETURNS INT IMMUTABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN (CASE WHEN _nai = 'HBD' THEN 13 WHEN _nai = 'HIVE' THEN 21 ELSE 37 END);
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.get_liquid_nai_type(_nai btracker_backend.liquid_nai_type)
RETURNS INT IMMUTABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN (CASE WHEN "coin-type" = 'HBD' THEN 13 ELSE 21 END);
END
$$;

RESET ROLE;
