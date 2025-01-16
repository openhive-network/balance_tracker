SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_withdraw_vesting_operation(
    body jsonb, _withdraw_rate int
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH withdraw_vesting_operation AS
(
  SELECT 
    (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'account') AS _account,
    GREATEST(((body)->'value'->'vesting_shares'->>'amount')::BIGINT, 0) AS _vesting_withdraw
)
INSERT INTO account_withdraws
  (
    account,
    vesting_withdraw_rate,
    to_withdraw
  )
  SELECT 
    _account,
    _vesting_withdraw / _withdraw_rate,
    _vesting_withdraw
  FROM withdraw_vesting_operation

  ON CONFLICT ON CONSTRAINT pk_account_withdraws
  DO UPDATE SET
      withdrawn = 0,
      vesting_withdraw_rate = EXCLUDED.vesting_withdraw_rate,
      to_withdraw = EXCLUDED.to_withdraw;
END
$$;

CREATE OR REPLACE FUNCTION process_set_withdraw_vesting_route_operation(
    body jsonb
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _account INT;
  _to_account INT;
  _percent INT;
  _current_balance INT;
BEGIN

SELECT (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'from_account'),
       (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'to_account'),
       ((body)->'value'->>'percent')::INT
INTO _account, _to_account, _percent;

  SELECT cawr.percent INTO _current_balance
  FROM account_routes cawr
  WHERE cawr.account=  _account
  AND cawr.to_account=  _to_account;
  
IF _current_balance IS NULL THEN

 INSERT INTO account_routes
    (
      account,
      to_account,
      percent
      )
      SELECT 
        _account,
        _to_account,
        _percent;

 INSERT INTO account_withdraws
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        1

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          withdraw_routes = account_withdraws.withdraw_routes + EXCLUDED.withdraw_routes;
ELSE
  IF _percent = 0 THEN

  INSERT INTO account_withdraws
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        1

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          withdraw_routes = account_withdraws.withdraw_routes - EXCLUDED.withdraw_routes;

  DELETE FROM account_routes
  WHERE account = _account
  AND to_account = _to_account;

  ELSE

  UPDATE account_routes SET
    percent = _percent
  WHERE account = _account 
  AND to_account = _to_account;

  END IF;
END IF;
END
$$;

CREATE OR REPLACE FUNCTION process_fill_vesting_withdraw_operation(
    body jsonb,
    _start_delayed_vests boolean
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
_vesting_withdraw BIGINT;
_account INT;
_vesting_deposit BIGINT;
_precision INT;
_to_account INT;
BEGIN

  SELECT (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'from_account'),
        (SELECT id FROM accounts_view WHERE name = (body)->'value'->>'to_account'),
        ((body)->'value'->'withdrawn'->>'amount')::BIGINT,
        ((body)->'value'->'deposited'->>'amount')::BIGINT,
        ((body)->'value'->'deposited'->>'precision')::INT
  INTO _account, _to_account, _vesting_withdraw, _vesting_deposit, _precision;

  INSERT INTO account_withdraws
  (
    account,
    withdrawn
    )
    SELECT 
      _account,
      _vesting_withdraw

    ON CONFLICT ON CONSTRAINT pk_account_withdraws
    DO UPDATE SET
        withdrawn = account_withdraws.withdrawn + EXCLUDED.withdrawn;

  IF _start_delayed_vests = TRUE THEN

    INSERT INTO account_withdraws
    (
      account,
      delayed_vests
      )
      SELECT 
        _account,
        _vesting_withdraw

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          delayed_vests = GREATEST(account_withdraws.delayed_vests - EXCLUDED.delayed_vests, 0);
    
    IF _precision = 6 THEN

      INSERT INTO account_withdraws
      (
      account,
      delayed_vests
      )
      SELECT 
        _to_account,
        _vesting_deposit

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          delayed_vests = account_withdraws.delayed_vests + EXCLUDED.delayed_vests;

    END IF;

  END IF;

  UPDATE account_withdraws SET
    vesting_withdraw_rate = 0,
    to_withdraw = 0,
    withdrawn = 0
  WHERE account_withdraws.account = _account
  AND account_withdraws.to_withdraw = account_withdraws.withdrawn;
END
$$;

RESET ROLE;
