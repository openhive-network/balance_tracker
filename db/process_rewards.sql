SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_claim_reward_balance_operation(
    body jsonb, _source_op bigint, _source_op_block int
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH claim_reward_balance_operation AS 
(
  SELECT 
    (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'account') AS _account,
    ((body)->'value'->'reward_hive'->>'amount')::BIGINT AS _hive_payout,
    ((body)->'value'->'reward_hbd'->>'amount')::BIGINT AS _hbd_payout,
    ((body)->'value'->'reward_vests'->>'amount')::numeric AS _vesting_payout
)
  INSERT INTO btracker_app.account_rewards
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
  FROM btracker_app.account_rewards car
  JOIN btracker_app.account_rewards caar ON caar.nai = 38 AND caar.account = car.account 
  WHERE car.nai = 37 and car.account = crbo._account),
    _source_op,
    _source_op_block
  FROM claim_reward_balance_operation crbo
  WHERE crbo._vesting_payout > 0
  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
      balance = btracker_app.account_rewards.balance - EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block;

END
$$;


CREATE OR REPLACE FUNCTION btracker_app.process_author_reward_operation(
    body jsonb,
    _source_op bigint,
    _source_op_block int
)

RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  WITH author_reward_operation AS 
  (
    SELECT
      (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'author') AS _account,
      ((body)->'value'->'hbd_payout'->>'amount')::BIGINT AS _hbd_payout,
      ((body)->'value'->'hive_payout'->>'amount')::BIGINT AS _hive_payout,
      ((body)->'value'->'vesting_payout'->>'amount')::BIGINT AS _vesting_payout
  )
  INSERT INTO btracker_app.account_rewards (
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
  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
    balance = btracker_app.account_rewards.balance + EXCLUDED.balance,
    source_op = EXCLUDED.source_op,
    source_op_block = EXCLUDED.source_op_block;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_curation_reward_operation(
    body jsonb,
    _source_op bigint,
    _source_op_block int
)

RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  WITH curation_reward_operation AS 
  (
    SELECT 
      (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'curator') AS _account,
      ((body)->'value'->'reward'->>'amount')::BIGINT AS _reward,
      (SELECT hive.get_vesting_balance(_source_op_block, ((body)->'value'->'reward'->>'amount')::BIGINT)) AS _reward_hive,
      ((body)->'value'->>'payout_must_be_claimed')::BOOLEAN AS _payout_must_be_claimed
  ),
  insert_balance AS
  (
INSERT INTO btracker_app.account_rewards (
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
    WHERE _payout_must_be_claimed = TRUE

  UNION ALL
  SELECT
    _account,
    38,
    _reward_hive,
    _source_op,
    _source_op_block
    FROM curation_reward_operation
    WHERE _payout_must_be_claimed = TRUE

  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
      balance = btracker_app.account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block
  )

  INSERT INTO btracker_app.account_info_rewards (
    account,
    curation_rewards
  )
  SELECT
    _account,
    _reward_hive
    FROM curation_reward_operation

  ON CONFLICT ON CONSTRAINT pk_account_info_rewards
  DO UPDATE SET
    curation_rewards = btracker_app.account_info_rewards.curation_rewards + EXCLUDED.curation_rewards;
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_comment_benefactor_reward_operation(
    body jsonb, _source_op bigint, _source_op_block int
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH comment_benefactor_reward_operation AS 
(
  SELECT 
    (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'benefactor') AS _account,
    ((body)->'value'->'hbd_payout'->>'amount')::BIGINT AS _hbd_payout,
    ((body)->'value'->'hive_payout'->>'amount')::BIGINT AS _hive_payout,
    ((body)->'value'->'vesting_payout'->>'amount')::BIGINT AS _vesting_payout
)
INSERT INTO btracker_app.account_rewards (
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
  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
    balance = btracker_app.account_rewards.balance + EXCLUDED.balance,
    source_op = EXCLUDED.source_op,
    source_op_block = EXCLUDED.source_op_block;
END
$$;

CREATE OR REPLACE FUNCTION btracker_app.process_comment_reward_operation(
    body jsonb
)
RETURNS void
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN

WITH comment_reward_operation AS
(
  SELECT 
    (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'author') AS _account,
    ((body)->'value'->>'author_rewards')::BIGINT AS _author_rewards
)
INSERT INTO btracker_app.account_info_rewards (
    account,
    posting_rewards
  )
  SELECT
    _account,
    _author_rewards
  FROM comment_reward_operation

  ON CONFLICT ON CONSTRAINT pk_account_info_rewards
  DO UPDATE SET
    posting_rewards = btracker_app.account_info_rewards.posting_rewards + EXCLUDED.posting_rewards;

END
$$;

RESET ROLE;
