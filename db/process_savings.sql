CREATE OR REPLACE FUNCTION btracker_app.process_transfer_to_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH transfer_to_savings_operation AS  
(
SELECT (body::jsonb)->'value'->>'to' AS _account,
       substring((body::jsonb)->'value'->'amount'->>'nai', '[0-9]+')::INT AS _nai,
       ((body::jsonb)->'value'->'amount'->>'amount')::BIGINT AS _balance
)
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
      FROM transfer_to_savings_operation

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
        1

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
BEGIN
WITH cancel_transfer_from_savings_operation AS 
(
SELECT account AS _account , nai AS _nai, balance AS _balance
FROM btracker_app.transfer_saving_id
WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account= ((body::jsonb)->'value'->>'from')
)
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
        1
      FROM cancel_transfer_from_savings_operation

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance + EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block,
          savings_withdraw_requests = btracker_app.current_account_savings.savings_withdraw_requests - EXCLUDED.savings_withdraw_requests;

  DELETE FROM btracker_app.transfer_saving_id
  WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account = ((body::jsonb)->'value'->>'from');

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_fill_transfer_from_savings_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH fill_transfer_from_savings_operation AS 
(
SELECT (body::jsonb)->'value'->>'from' AS _account,
       substring((body::jsonb)->'value'->'amount'->>'nai', '[0-9]+')::INT AS _nai
)
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
        1
      FROM fill_transfer_from_savings_operation

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block,
          savings_withdraw_requests = btracker_app.current_account_savings.savings_withdraw_requests - EXCLUDED.savings_withdraw_requests;

  DELETE FROM btracker_app.transfer_saving_id
  WHERE request_id = ((body::jsonb)->'value'->>'request_id')::BIGINT AND account = ((body::jsonb)->'value'->>'from');

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_interest_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH interest_operation AS 
(
SELECT (body::jsonb)->'value'->>'owner' AS _account, 
       substring((body::jsonb)->'value'->'interest'->>'nai', '[0-9]+')::INT AS _nai,
       ((body::jsonb)->'value'->'interest'->>'amount')::BIGINT AS _balance
)
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
      FROM interest_operation

      ON CONFLICT ON CONSTRAINT pk_account_savings
      DO UPDATE SET
          saving_balance = btracker_app.current_account_savings.saving_balance + EXCLUDED.saving_balance,
          source_op = EXCLUDED.source_op,
          source_op_block = EXCLUDED.source_op_block;

END
$$
;
