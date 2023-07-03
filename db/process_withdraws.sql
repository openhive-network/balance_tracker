CREATE OR REPLACE FUNCTION btracker_app.process_withdraw_vesting_operation(body hive.operation, _withdraw_rate INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH withdraw_vesting_operation AS
(
SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body::jsonb)->'value'->>'account') AS _account,
       ((body::jsonb)->'value'->'vesting_shares'->>'amount')::BIGINT AS _vesting_withdraw
)
 INSERT INTO btracker_app.current_account_withdraws 
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

      ON CONFLICT ON CONSTRAINT pk_current_account_withdraws
      DO UPDATE SET
          withdrawn = 0,
          vesting_withdraw_rate = EXCLUDED.vesting_withdraw_rate,
          to_withdraw = EXCLUDED.to_withdraw;

END
$$
;

 CREATE OR REPLACE FUNCTION btracker_app.process_set_withdraw_vesting_route_operation(body hive.operation)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account INT;
  _to_account INT;
  _percent INT;
  _routes INT := 1;
  _current_balance INT;
BEGIN

SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body::jsonb)->'value'->>'from_account'),
       (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body::jsonb)->'value'->>'to_account'),
       ((body::jsonb)->'value'->>'percent')::INT
INTO _account, _to_account, _percent;

  SELECT cawr.percent INTO _current_balance
  FROM btracker_app.current_account_withdraws_routes cawr 
  WHERE cawr.account=  _account
  AND cawr.to_account=  _to_account;
  
IF _current_balance IS NULL THEN

 INSERT INTO btracker_app.current_account_withdraws_routes 
    (
      account,
      to_account,
      percent
      )
      SELECT 
        _account,
        _to_account,
        _percent;

 INSERT INTO btracker_app.current_account_withdraws 
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        _routes

      ON CONFLICT ON CONSTRAINT pk_current_account_withdraws
      DO UPDATE SET
          withdraw_routes = btracker_app.current_account_withdraws.withdraw_routes + EXCLUDED.withdraw_routes;
ELSE
  IF _percent = 0 THEN

  INSERT INTO btracker_app.current_account_withdraws 
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        _routes

      ON CONFLICT ON CONSTRAINT pk_current_account_withdraws
      DO UPDATE SET
          withdraw_routes = btracker_app.current_account_withdraws.withdraw_routes - EXCLUDED.withdraw_routes;

  DELETE FROM btracker_app.current_account_withdraws_routes
  WHERE account = _account
  AND to_account = _to_account;

  ELSE

  UPDATE btracker_app.current_account_withdraws_routes SET 
    percent = _percent
  WHERE account = _account 
  AND to_account = _to_account;

  END IF;
END IF;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_fill_vesting_withdraw_operation(body hive.operation)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
_account INT;
_vesting_withdraw BIGINT;
BEGIN

SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body::jsonb)->'value'->>'from_account'),
       ((body::jsonb)->'value'->'withdrawn'->>'amount')::BIGINT
INTO _account, _vesting_withdraw;

 INSERT INTO btracker_app.current_account_withdraws 
    (
      account,
      withdrawn
      )
      SELECT 
        _account,
        _vesting_withdraw

      ON CONFLICT ON CONSTRAINT pk_current_account_withdraws
      DO UPDATE SET
          withdrawn = btracker_app.current_account_withdraws.withdrawn + EXCLUDED.withdrawn;

    UPDATE btracker_app.current_account_withdraws SET 
      vesting_withdraw_rate = 0,
      to_withdraw = 0,
      withdrawn = 0
    WHERE account = _account AND to_withdraw <= withdrawn + 1;

END
$$
;
