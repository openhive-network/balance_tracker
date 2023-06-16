CREATE OR REPLACE FUNCTION btracker_app.process_claim_reward_balance_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout BIGINT;
  _nai_hbd BIGINT := 13;
  _nai_hive BIGINT := 21;
  _nai_vest BIGINT := 37;
BEGIN

SELECT (body::jsonb)->'value'->>'account',
       ((body::jsonb)->'value'->'reward_hive'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'reward_hbd'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'reward_vests'->>'amount')::BIGINT
INTO _account, _hive_payout, _hbd_payout, _vesting_payout;

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  VALUES
    (_account,
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_hive,
    _hive_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block)

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_author_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _is_false BOOLEAN;
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout BIGINT;
  _nai_hbd BIGINT := 13;
  _nai_hive BIGINT := 21;
  _nai_vest BIGINT := 37;

BEGIN

SELECT (body::jsonb)->'value'->>'author',
       ((body::jsonb)->'value'->>'payout_must_be_claimed')::BOOLEAN,
       ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT
INTO _account, _is_false, _hbd_payout, _hive_payout, _vesting_payout;

IF _is_false = FALSE THEN

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  VALUES
    (_account,
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_hive,
    _hive_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block)

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END IF;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_curation_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _is_false BOOLEAN;
  _reward BIGINT;
  _nai INT;
BEGIN

SELECT (body::jsonb)->'value'->>'curator',
       ((body::jsonb)->'value'->>'payout_must_be_claimed')::BOOLEAN,
       ((body::jsonb)->'value'->'reward'->>'amount')::BIGINT,
       substring((body::jsonb)->'value'->'reward'->>'nai', '[0-9]+')::INT
INTO _account, _is_false, _reward, _nai;

IF _is_false = FALSE THEN

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  SELECT
    _account,
    _nai,
    _reward,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END IF;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_comment_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _hive_payout BIGINT;
  _nai_hive INT := 21;
BEGIN

SELECT (body::jsonb)->'value'->>'author',
       ((body::jsonb)->'value'->>'author_rewards')::BIGINT
INTO _account, _hive_payout;

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  SELECT
    _account,
    _nai_hive,
    _hive_payout,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_liquidity_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _reward BIGINT;
  _nai INT;
BEGIN

SELECT (body::jsonb)->'value'->>'owner',
       ((body::jsonb)->'value'->'payout'->>'amount')::BIGINT,
       substring((body::jsonb)->'value'->'payout'->>'nai', '[0-9]+')::INT
INTO _account, _reward, _nai;

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  SELECT
    _account,
    _nai,
    _reward,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_comment_benefactor_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _is_false BOOLEAN;
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout BIGINT;
  _nai_hbd BIGINT := 13;
  _nai_hive BIGINT := 21;
  _nai_vest BIGINT := 37;
BEGIN

SELECT (body::jsonb)->'value'->>'benefactor',
       ((body::jsonb)->'value'->>'payout_must_be_claimed')::BOOLEAN,
       ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT
INTO _account, _is_false, _hbd_payout, _hive_payout, _vesting_payout;

IF _is_false = FALSE THEN

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  VALUES
    (_account,
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_hive,
    _hive_payout,
    _source_op,
    _source_op_block),

    (_account,
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block)

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END IF;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_producer_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _reward BIGINT;
  _nai INT;
BEGIN

SELECT (body::jsonb)->'value'->>'producer',
       ((body::jsonb)->'value'->'vesting_shares'->>'amount')::BIGINT,
       substring((body::jsonb)->'value'->'vesting_shares'->>'nai', '[0-9]+')::INT
INTO _account, _reward, _nai;

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  SELECT
    _account,
    _nai,
    _reward,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_pow_reward_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _reward BIGINT;
  _nai INT;
BEGIN

SELECT (body::jsonb)->'value'->>'worker',
       ((body::jsonb)->'value'->'reward'->>'amount')::BIGINT,
       substring((body::jsonb)->'value'->'reward'->>'nai', '[0-9]+')::INT
INTO _account, _reward, _nai;

  INSERT INTO btracker_app.current_account_rewards
  (
  account,
  nai,
  balance,
  source_op,
  source_op_block
  ) 
  SELECT
    _account,
    _nai,
    _reward,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$
;
