CREATE OR REPLACE FUNCTION btracker_app.process_claim_reward_balance_operation(body hive.operation, _source_op BIGINT, _source_op_block INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH claim_reward_balance_operation AS MATERIALIZED
(
SELECT (body::jsonb)->'value'->>'account' AS _account,
       ((body::jsonb)->'value'->'reward_hive'->>'amount')::BIGINT AS _hive_payout,
       ((body::jsonb)->'value'->'reward_hbd'->>'amount')::BIGINT AS _hbd_payout,
       ((body::jsonb)->'value'->'reward_vests'->>'amount')::numeric AS _vesting_payout
)
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
    13,
    _hbd_payout,
    _source_op,
    _source_op_block
  FROM claim_reward_balance_operation
  WHERE _hbd_payout > 0
  UNION ALL
  SELECT
    _account,
    21,
    _hive_payout,
    _source_op,
    _source_op_block
  FROM claim_reward_balance_operation
  WHERE _hive_payout > 0
  UNION ALL
  SELECT
    _account,
    37,
    _vesting_payout,
    _source_op,
    _source_op_block
  FROM claim_reward_balance_operation
  WHERE _vesting_payout > 0
  UNION ALL
  SELECT
  crbo._account,
  38,
  (SELECT ROUND(caar.balance * crbo._vesting_payout / car.balance, 3)
  FROM btracker_app.current_account_rewards car
  JOIN btracker_app.current_account_rewards caar ON caar.nai = 38 AND caar.account = car.account 
  WHERE car.nai = 37 and car.account = crbo._account),
  _source_op,
  _source_op_block
  FROM claim_reward_balance_operation crbo
  WHERE crbo._vesting_payout > 0   
  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
      balance = btracker_app.current_account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;
END
$$
;


CREATE OR REPLACE FUNCTION btracker_app.process_author_reward_operation(
  body hive.operation,
  _source_op BIGINT,
  _source_op_block INT
)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  WITH author_reward_operation AS MATERIALIZED
  (
    SELECT
      (body::jsonb)->'value'->>'author' AS _account,
      ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT AS _hbd_payout,
      ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT AS _hive_payout,
      ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT AS _vesting_payout
  )
  INSERT INTO btracker_app.current_account_rewards (
    account,
    nai,
    balance,
    source_op,
    source_op_block
  )
  SELECT
    _account,
    13,
    _hbd_payout,
    _source_op,
    _source_op_block
  FROM author_reward_operation
  WHERE _hbd_payout > 0
  UNION ALL
  SELECT
    _account,
    21,
    _hive_payout,
    _source_op,
    _source_op_block
  FROM author_reward_operation
  WHERE _hive_payout > 0
  UNION ALL
  SELECT
    _account,
    37,
    _vesting_payout,
    _source_op,
    _source_op_block
  FROM author_reward_operation
  WHERE _vesting_payout > 0
  UNION ALL
  SELECT
    _account,
    38,
    (SELECT hive.get_vesting_balance(_source_op_block, _vesting_payout)),
    _source_op,
    _source_op_block
  FROM author_reward_operation
  WHERE _vesting_payout > 0
  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
    balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
    source_op = EXCLUDED.source_op,
    source_op_block = EXCLUDED.source_op_block;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_curation_reward_operation(
  body hive.operation, 
  _source_op BIGINT, 
  _source_op_block INT
)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
WITH curation_reward_operation AS MATERIALIZED
(
SELECT (body::jsonb)->'value'->>'curator' AS _account,
       ((body::jsonb)->'value'->'reward'->>'amount')::BIGINT AS _reward
)
INSERT INTO btracker_app.current_account_rewards (
    account,
    nai,
    balance,
    source_op,
    source_op_block
  )
  SELECT
    _account,
    37,
    _reward,
    _source_op,
    _source_op_block
  FROM curation_reward_operation
  UNION ALL
  SELECT
    _account,
    38,
    (SELECT hive.get_vesting_balance(_source_op_block, _reward)),
    _source_op,
    _source_op_block
  FROM curation_reward_operation
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
BEGIN
WITH comment_benefactor_reward_operation AS MATERIALIZED
(
SELECT (body::jsonb)->'value'->>'benefactor' AS _account,
       ((body::jsonb)->'value'->'hbd_payout'->>'amount')::BIGINT AS _hbd_payout,
       ((body::jsonb)->'value'->'hive_payout'->>'amount')::BIGINT AS _hive_payout,
       ((body::jsonb)->'value'->'vesting_payout'->>'amount')::BIGINT AS _vesting_payout
)
INSERT INTO btracker_app.current_account_rewards (
    account,
    nai,
    balance,
    source_op,
    source_op_block
  )
  SELECT
    _account,
    13,
    _hbd_payout,
    _source_op,
    _source_op_block
  FROM comment_benefactor_reward_operation
  WHERE _hbd_payout > 0
  UNION ALL
  SELECT
    _account,
    21,
    _hive_payout,
    _source_op,
    _source_op_block
  FROM comment_benefactor_reward_operation
  WHERE _hive_payout > 0
  UNION ALL
  SELECT
    _account,
    37,
    _vesting_payout,
    _source_op,
    _source_op_block
  FROM comment_benefactor_reward_operation
  WHERE _vesting_payout > 0
  UNION ALL
  SELECT
    _account,
    38,
    (SELECT hive.get_vesting_balance(_source_op_block, _vesting_payout)),
    _source_op,
    _source_op_block
  FROM comment_benefactor_reward_operation
  WHERE _vesting_payout > 0
  ON CONFLICT ON CONSTRAINT pk_current_account_rewards
  DO UPDATE SET
    balance = btracker_app.current_account_rewards.balance + EXCLUDED.balance,
    source_op = EXCLUDED.source_op,
    source_op_block = EXCLUDED.source_op_block;
END
$$
;
