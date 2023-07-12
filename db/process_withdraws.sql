CREATE OR REPLACE FUNCTION btracker_app.process_withdraw_vesting_operation(body jsonb, _withdraw_rate INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH withdraw_vesting_operation AS
(
SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'account') AS _account,
       ((body)->'value'->'vesting_shares'->>'amount')::BIGINT AS _vesting_withdraw
)
 INSERT INTO btracker_app.account_withdraws 
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
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_set_withdraw_vesting_route_operation(body jsonb)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account INT;
  _to_account INT;
  _percent INT;
  _current_balance INT;
BEGIN

SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'from_account'),
       (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'to_account'),
       ((body)->'value'->>'percent')::INT
INTO _account, _to_account, _percent;

  SELECT cawr.percent INTO _current_balance
  FROM btracker_app.account_routes cawr 
  WHERE cawr.account=  _account
  AND cawr.to_account=  _to_account;
  
IF _current_balance IS NULL THEN

 INSERT INTO btracker_app.account_routes 
    (
      account,
      to_account,
      percent
      )
      SELECT 
        _account,
        _to_account,
        _percent;

 INSERT INTO btracker_app.account_withdraws 
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        1

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          withdraw_routes = btracker_app.account_withdraws.withdraw_routes + EXCLUDED.withdraw_routes;
ELSE
  IF _percent = 0 THEN

  INSERT INTO btracker_app.account_withdraws 
    (
      account,
      withdraw_routes
      )
      SELECT 
        _account,
        1

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          withdraw_routes = btracker_app.account_withdraws.withdraw_routes - EXCLUDED.withdraw_routes;

  DELETE FROM btracker_app.account_routes
  WHERE account = _account
  AND to_account = _to_account;

  ELSE

  UPDATE btracker_app.account_routes SET 
    percent = _percent
  WHERE account = _account 
  AND to_account = _to_account;

  END IF;
END IF;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_fill_vesting_withdraw_operation(body jsonb)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH fill_vesting_withdraw_operation AS
(
SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'from_account') AS _account,
       ((body)->'value'->'withdrawn'->>'amount')::BIGINT AS _vesting_withdraw
),
insert_balance AS
(
 INSERT INTO btracker_app.account_withdraws 
    (
      account,
      withdrawn
      )
      SELECT 
        _account,
        _vesting_withdraw
      FROM fill_vesting_withdraw_operation

      ON CONFLICT ON CONSTRAINT pk_account_withdraws
      DO UPDATE SET
          withdrawn = btracker_app.account_withdraws.withdrawn + EXCLUDED.withdrawn
      RETURNING account
)
    UPDATE btracker_app.account_withdraws SET 
      vesting_withdraw_rate = 0,
      to_withdraw = 0,
      withdrawn = 0
    FROM insert_balance
    WHERE btracker_app.account_withdraws.account = insert_balance.account 
    AND btracker_app.account_withdraws.to_withdraw <= btracker_app.account_withdraws.withdrawn + 1;
END
$$
;
