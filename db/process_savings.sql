CREATE OR REPLACE FUNCTION btracker_app.process_transfer_to_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _account TEXT;
    _nai INT;
    _balance BIGINT;
BEGIN

SELECT (body::jsonb)->'value'->>'to',
       substring((body::jsonb)->'value'->'amount'->>'nai', '[0-9]+')::INT,
       ((body::jsonb)->'value'->'amount'->>'amount')::BIGINT
INTO _account, _nai, _balance;

      INSERT INTO btracker_app.current_account_savings
      (
      account,
      nai,
      saving_balance,
      source_op,
      source_op_block
      ) 
      SELECT
        _account,
        _nai,
        _balance,
        _source_op,
        _source_op_block

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance + EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_transfer_from_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _account TEXT;
    _request_id BIGINT;
    _nai INT;
    _balance BIGINT;
    _savings_withdraw_requests INT := 1;
BEGIN

SELECT (body::jsonb)->'value'->>'from',
       ((body::jsonb)->'value'->>'request_id')::BIGINT,
       substring((body::jsonb)->'value'->'amount'->>'nai', '[0-9]+')::INT,
       ((body::jsonb)->'value'->'amount'->>'amount')::BIGINT
INTO _account, _request_id, _nai, _balance;

    INSERT INTO btracker_app.current_account_savings 
    (
      account,
      nai,
      saving_balance,
      source_op,
      source_op_block,
      savings_withdraw_requests
      )
      SELECT 
        _account,
        _nai,
        _balance,
        _source_op,
        _source_op_block,
        _savings_withdraw_requests

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance - EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block,
          savings_withdraw_requests = btracker_app.current_account_savings.savings_withdraw_requests + EXCLUDED.savings_withdraw_requests;

      INSERT INTO btracker_app.transfer_saving_id
      (
      account,
      nai,
      balance,
      request_id
      ) 
      SELECT
        _account,
        _nai,
        _balance,
        _request_id;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_cancel_transfer_from_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _account TEXT;
    _nai INT;
    _balance BIGINT;
    _savings_withdraw_requests INT := 1;
BEGIN

SELECT account, nai, balance 
INTO _account, _nai, _balance
FROM btracker_app.transfer_saving_id
WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account= ((body::jsonb)->'value'->>'from');

    INSERT INTO btracker_app.current_account_savings 
    (
      account,
      nai,
      saving_balance,
      source_op,
      source_op_block,
      savings_withdraw_requests
      )
      SELECT 
        _account,
        _nai,
        _balance,
        _source_op,
        _source_op_block,
        _savings_withdraw_requests

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance + EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block,
          savings_withdraw_requests = btracker_app.current_account_savings.savings_withdraw_requests - EXCLUDED.savings_withdraw_requests;


  DELETE FROM btracker_app.transfer_saving_id
  WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account = _account;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_fill_transfer_from_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _account TEXT;
    _request_id BIGINT;
    _nai INT;
    _balance BIGINT;
    _savings_withdraw_requests INT := 1;
BEGIN

SELECT (body::jsonb)->'value'->>'from',
       ((body::jsonb)->'value'->>'request_id')::BIGINT,
       substring((body::jsonb)->'value'->'amount'->>'nai', '[0-9]+')::INT
INTO _account, _request_id, _nai;

    INSERT INTO btracker_app.current_account_savings 
    (
      account,
      nai,
      source_op,
      source_op_block,
      savings_withdraw_requests
      )
      SELECT 
        _account,
        _nai,
        _source_op,
        _source_op_block,
        _savings_withdraw_requests
      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block,
          savings_withdraw_requests = btracker_app.current_account_savings.savings_withdraw_requests - EXCLUDED.savings_withdraw_requests;

  DELETE FROM btracker_app.transfer_saving_id
  WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account = _account;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_interest_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    _account TEXT;
    _is_false BOOLEAN;
    _nai INT;
    _balance BIGINT;
BEGIN

SELECT (body::jsonb)->'value'->>'owner',
       ((body::jsonb)->'value'->>'is_saved_into_hbd_balance')::BOOLEAN,
       substring((body::jsonb)->'value'->'interest'->>'nai', '[0-9]+')::INT,
       ((body::jsonb)->'value'->'interest'->>'amount')::BIGINT
INTO _account, _is_false, _nai, _balance;

IF _is_false= FALSE THEN
    INSERT INTO btracker_app.current_account_savings 
    (
      account,
      nai,
      saving_balance,
      source_op,
      source_op_block
      )
      SELECT 
        _account,
        _nai,
        _balance,
        _source_op,
        _source_op_block
      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance + EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block;
END IF;

END
$$
;
