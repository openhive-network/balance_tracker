CREATE OR REPLACE FUNCTION btracker_app.process_delegate_vesting_shares_operation(body jsonb, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _delegator INT;
    _delegatee INT;
    _balance BIGINT;
    _current_balance BIGINT;
    ___current_blocked_balance BIGINT;
BEGIN

SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'delegator'),
       (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'delegatee'),
       ((body)->'value'->'vesting_shares'->>'amount')::BIGINT
INTO _delegator, _delegatee, _balance;

--  DELEGATIONS TRACKING
    SELECT cad.balance INTO _current_balance
    FROM btracker_app.current_accounts_delegations cad 
    WHERE cad.delegator= _delegator AND cad.delegatee= _delegatee;
    
  IF _current_balance IS NULL THEN

  --IF DELEGATION BETWEEN THIS PAIR NEVER HAPPENED BEFORE

  --UPDATE CURRENT_ACCOUNTS_DELEGATIONS
    INSERT INTO btracker_app.current_accounts_delegations 
    (
      delegator,
      delegatee,
      balance,
      source_op,
      source_op_block
      )
      SELECT 
        _delegator,
        _delegatee,
        _balance,
        _source_op,
        _source_op_block;

  --ADD DELEGATED VESTS TO DELEGATOR
      INSERT INTO btracker_app.current_account_vests
      (
      account,
      delegated_vests
      ) 
      SELECT
        _delegator,
        _balance
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          delegated_vests = btracker_app.current_account_vests.delegated_vests + EXCLUDED.delegated_vests;

  --ADD RECEIVED VESTS TO DELEGATEE
      INSERT INTO btracker_app.current_account_vests
      (
      account,
      received_vests     
      ) 
      SELECT
        _delegatee,
        _balance
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          received_vests = btracker_app.current_account_vests.received_vests + EXCLUDED.received_vests;

    ELSE

    UPDATE btracker_app.current_accounts_delegations SET 
      balance = _balance,
      source_op = _source_op,
      source_op_block = _source_op_block
    WHERE delegator = _delegator AND delegatee = _delegatee;

  IF _current_balance > _balance THEN

  --IF DELEGATION BETWEEN ACCOUNTS HAPPENED BUT THE DELEGATION IS LOWER THAN PREVIOUS DELEGATION

  ___current_blocked_balance = GREATEST(_current_balance - _balance , 0);

  
  --DELEGATEE'S RECEIVED VESTS ARE BEING LOWERED INSTANTLY

      INSERT INTO btracker_app.current_account_vests
      (
      account,
      received_vests     
      ) 
      SELECT
        _delegatee,
        ___current_blocked_balance
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          received_vests = btracker_app.current_account_vests.received_vests - EXCLUDED.received_vests;
  ELSE

  --IF DELEGATION BETWEEN ACCOUNTS HAPPENED BUT THE DELEGATION IS HIGHER
      
    ___current_blocked_balance = GREATEST(_balance - _current_balance, 0);
    
  --ADD THE DIFFERENCE TO BOTH ACCOUNTS DELEGATED AND RECEIVED

      INSERT INTO btracker_app.current_account_vests
      (
      account,
      delegated_vests
      ) 
      SELECT
        _delegator,
        ___current_blocked_balance
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          delegated_vests = btracker_app.current_account_vests.delegated_vests + EXCLUDED.delegated_vests;
  
      INSERT INTO btracker_app.current_account_vests
      (
      account,
      received_vests     
      ) 
      SELECT
        _delegatee,
        ___current_blocked_balance
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          received_vests = btracker_app.current_account_vests.received_vests + EXCLUDED.received_vests;
    
  END IF;

  END IF;

  --IF DELEGATION IS BEING CANCELED BETWEEN ACCOUNTS REMOVE IT FROM CURRENT_ACCOUNT_DELEGATION

  IF _balance = 0 THEN
    DELETE FROM btracker_app.current_accounts_delegations
    WHERE delegator = _delegator AND delegatee = _delegatee;
  END IF;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_account_create_with_delegation_operation(body jsonb, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH account_create_with_delegation_operation AS 
(
SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'creator') AS _delegator,
       (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'new_account_name')AS _delegatee,
       ((body)->'value'->'delegation'->>'amount')::BIGINT AS _balance
),
create_delegation AS 
(
    INSERT INTO btracker_app.current_accounts_delegations 
    (
      delegator,
      delegatee,
      balance,
      source_op,
      source_op_block
      )
      SELECT 
        _delegator,
        _delegatee,
        _balance,
        _source_op,
        _source_op_block
      FROM account_create_with_delegation_operation
),
increase_delegations AS 
(
      INSERT INTO btracker_app.current_account_vests
      (
      account,
      delegated_vests
      ) 
      SELECT
        _delegator,
        _balance
      FROM account_create_with_delegation_operation
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          delegated_vests = btracker_app.current_account_vests.delegated_vests + EXCLUDED.delegated_vests
)
      INSERT INTO btracker_app.current_account_vests
      (
      account,
      received_vests     
      ) 
      SELECT
        _delegatee,
        _balance
      FROM account_create_with_delegation_operation
      ON CONFLICT ON CONSTRAINT pk_temp_vests
      DO UPDATE SET
          received_vests = btracker_app.current_account_vests.received_vests + EXCLUDED.received_vests;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_return_vesting_delegation_operation(body jsonb, source_op BIGINT, source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH return_vesting_delegation_operation AS 
(
SELECT (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'account') AS _account,
       ((body)->'value'->'vesting_shares'->>'amount')::BIGINT AS _balance
)

  INSERT INTO btracker_app.current_account_vests (account, delegated_vests)
  SELECT _account, _balance
  FROM return_vesting_delegation_operation

  ON CONFLICT ON CONSTRAINT pk_temp_vests DO UPDATE SET
    delegated_vests = btracker_app.current_account_vests.delegated_vests - EXCLUDED.delegated_vests;

END
$$
;
