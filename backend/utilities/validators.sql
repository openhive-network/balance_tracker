SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.validate_account(_account_id INT, _account_name TEXT, _required BOOLEAN)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF (_required AND _account_id IS NULL) OR (NOT _required AND _account_name IS NOT NULL AND _account_id IS NULL) THEN
    PERFORM btracker_backend.rest_raise_missing_account(_account_name);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.validate_balance_history(
    _balance_type btracker_backend.balance_type,
    _coin_type btracker_backend.nai_type
)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF _balance_type = 'savings_balance' AND _coin_type = 'VESTS' THEN
    PERFORM btracker_backend.rest_raise_vest_saving_balance(_balance_type, _coin_type);
  END IF;
END
$$;

RESET ROLE;
