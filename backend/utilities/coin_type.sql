SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_nai_type(_nai btracker_backend.nai_type)
RETURNS INT STABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN a.asset_symbol_nai FROM asset_table a WHERE a.asset_name = _nai::TEXT;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.create_amount_object(IN __nai INT, IN __amount BIGINT)
RETURNS btracker_backend.amount
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (a.nai_string, __amount::TEXT, a.asset_precision)::btracker_backend.amount
  FROM asset_table a
  WHERE a.asset_symbol_nai = __nai;
END
$$;

RESET ROLE;
