SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.get_account_id(_account_name TEXT, _required BOOLEAN)
RETURNS INT STABLE
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account_id INT := (SELECT av.id FROM hive.accounts_view av WHERE av.name = _account_name);
BEGIN
  PERFORM btracker_backend.validate_account(_account_id, _account_name, _required);

  RETURN _account_id;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.get_account_name(_account_id INT)
RETURNS TEXT STABLE
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN av.name FROM hive.accounts_view av WHERE av.id = _account_id;
END
$$;

RESET ROLE;
