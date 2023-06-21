CREATE OR REPLACE FUNCTION btracker_app.process_claim_reward_balance_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _account TEXT;
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout numeric;
  _nai_hbd INT := 13;
  _nai_hive INT := 21;
  _nai_vest INT := 37;
  _vesting_diff BIGINT;
BEGIN

SELECT (body::jsonb)->'value'->>'account',
       ((body::jsonb)->'value'->'reward_hive'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'reward_hbd'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'reward_vests'->>'amount')::BIGINT
INTO _account, _hive_payout, _hbd_payout, _vesting_payout;

IF _hbd_payout > 0 THEN

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
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

  END IF;

  IF _hive_payout > 0 THEN

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
      balance = btracker_app.current_account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

  END IF;

  IF _vesting_payout > 0 THEN

  SELECT ROUND(_vesting_payout / car.balance, 3) * caar.balance INTO _vesting_diff
  FROM btracker_app.current_account_rewards car
  JOIN btracker_app.current_account_rewards caar ON caar.nai = 38 AND caar.account = _account 
  WHERE car.nai = 37 and car.account = _account;

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
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block),

    (_account,
    38,
    _vesting_diff,
    _source_op,
    _source_op_block)

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;
  END IF;

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
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout BIGINT;
  _nai_hbd INT := 13;
  _nai_hive INT := 21;
  _nai_vest INT := 37;
  _reward_vesting_hive BIGINT;

BEGIN

SELECT (body::jsonb)->'value'->>'author',
       ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT
INTO _account, _hbd_payout, _hive_payout, _vesting_payout;

  IF _hbd_payout > 0 THEN

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
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

  END IF;

  IF _hive_payout > 0 THEN

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

  END IF;

  IF _vesting_payout > 0 THEN

  SELECT hive.get_vesting_balance(_source_op_block, _vesting_payout) 
  INTO _reward_vesting_hive;

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
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block),

    (_account,
    38,
    _reward_vesting_hive,
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
  _reward BIGINT;
  _nai INT;
  _reward_vesting_hive BIGINT;
BEGIN

SELECT (body::jsonb)->'value'->>'curator',
       ((body::jsonb)->'value'->'reward'->>'amount')::BIGINT,
       substring((body::jsonb)->'value'->'reward'->>'nai', '[0-9]+')::INT
INTO _account, _reward, _nai;

  SELECT hive.get_vesting_balance(_source_op_block, _reward) 
  INTO _reward_vesting_hive;

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
    _nai,
    _reward,
    _source_op,
    _source_op_block),

    (_account,
    38,
    _reward_vesting_hive,
    _source_op,
    _source_op_block)

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
  _hbd_payout BIGINT;
  _hive_payout BIGINT;
  _vesting_payout BIGINT;
  _nai_hbd INT := 13;
  _nai_hive INT := 21;
  _nai_vest INT := 37;
  _reward_vesting_hive BIGINT;
BEGIN

SELECT (body::jsonb)->'value'->>'benefactor',
       ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT,
       ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT
INTO _account, _hbd_payout, _hive_payout, _vesting_payout;

  IF _hbd_payout > 0 THEN

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
    _nai_hbd,
    _hbd_payout,
    _source_op,
    _source_op_block

  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

  END IF;

  IF _hive_payout > 0 THEN

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

  END IF;

  IF _vesting_payout > 0 THEN

  SELECT hive.get_vesting_balance(_source_op_block, _vesting_payout) 
  INTO _reward_vesting_hive;

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
    _nai_vest,
    _vesting_payout,
    _source_op,
    _source_op_block),

    (_account,
    38,
    _reward_vesting_hive,
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
