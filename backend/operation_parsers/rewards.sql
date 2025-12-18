/*
Operation parsers for reward operations.
Called by: db/process_rewards.sql

Extracts data from operation JSONBs for:
- comment_reward (posting rewards): Author rewards in HIVE (tracks total earned)
- curation_reward: Curator rewards in VESTS (tracks total earned)
- author_reward: Pending author rewards (HBD, HIVE, VESTS)
- comment_benefactor_reward: Benefactor share of author rewards
- claim_reward_balance: Claim pending rewards (negative amounts reduce pending balance)

Two return types:
- impacted_info_rewards_return: For lifetime reward tracking (account, reward amount, claim flag)
- impacted_rewards_return: For pending reward balance (account, hbd, hive, vesting amounts)
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_info_rewards_return CASCADE;
CREATE TYPE btracker_backend.impacted_info_rewards_return AS
(
    account_name           VARCHAR,
    reward                 BIGINT,
    payout_must_be_claimed BOOLEAN
);

DROP TYPE IF EXISTS btracker_backend.impacted_rewards_return CASCADE;
CREATE TYPE btracker_backend.impacted_rewards_return AS
(
    account_name   VARCHAR,
    hbd_payout     BIGINT,
    hive_payout    BIGINT,
    vesting_payout BIGINT
);

-- Extract comment_reward (posting rewards) data.
-- Tracks lifetime posting rewards earned (in HIVE). Always requires claim.
CREATE OR REPLACE FUNCTION btracker_backend.process_posting_rewards(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'author')::TEXT,      -- post author
    ((_operation_body)->'value'->>'author_rewards')::BIGINT,  -- HIVE amount
    TRUE                                                 -- payout_must_be_claimed
  )::btracker_backend.impacted_info_rewards_return;

END
$$;

-- Extract curation_reward data.
-- Tracks lifetime curation rewards earned (in VESTS). May auto-vest or require claim.
CREATE OR REPLACE FUNCTION btracker_backend.process_curation_rewards(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'curator')::TEXT,     -- voter who earned curation
    ((_operation_body)->'value'->'reward'->>'amount')::BIGINT,  -- VESTS amount
    ((_operation_body)->'value'->>'payout_must_be_claimed')::BOOLEAN  -- false if auto-vested
  )::btracker_backend.impacted_info_rewards_return;

END
$$;

-- Extract author_reward data (virtual op).
-- Pending rewards from post payouts (HBD, HIVE, VESTS). Adds to pending balance.
CREATE OR REPLACE FUNCTION btracker_backend.process_author_reward_operation(
    IN _operation_body JSONB,
    IN _source_op_block INT
)
RETURNS btracker_backend.impacted_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
RETURN (
  WITH author_reward_operation AS (
    SELECT
      ((_operation_body)->'value'->>'author')::TEXT AS account_name,
      ((_operation_body)->'value'->'hbd_payout'->>'amount')::BIGINT AS hbd_payout,     -- HBD portion
      ((_operation_body)->'value'->'hive_payout'->>'amount')::BIGINT AS hive_payout,   -- HIVE portion (if any)
      ((_operation_body)->'value'->'vesting_payout'->>'amount')::BIGINT AS vesting_payout  -- VESTS (HP) portion
  )
  SELECT
    (
      account_name,
      hbd_payout,
      hive_payout,
      vesting_payout
    )::btracker_backend.impacted_rewards_return
  FROM author_reward_operation
);

END
$$;

-- Extract comment_benefactor_reward data (virtual op).
-- Share of author rewards sent to designated beneficiary. Adds to pending balance.
CREATE OR REPLACE FUNCTION btracker_backend.process_benefactor_reward_operation(
    IN _operation_body JSONB,
    IN _source_op_block INT
)
RETURNS btracker_backend.impacted_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
RETURN (
  WITH benefactor_reward_operation AS (
    SELECT
      ((_operation_body)->'value'->>'benefactor')::TEXT AS account_name,  -- beneficiary account
      ((_operation_body)->'value'->'hbd_payout'->>'amount')::BIGINT AS hbd_payout,
      ((_operation_body)->'value'->'hive_payout'->>'amount')::BIGINT AS hive_payout,
      ((_operation_body)->'value'->'vesting_payout'->>'amount')::BIGINT AS vesting_payout
  )
  SELECT
    (
      account_name,
      hbd_payout,
      hive_payout,
      vesting_payout
    )::btracker_backend.impacted_rewards_return
  FROM benefactor_reward_operation
);

END
$$;

-- Extract claim_reward_balance data.
-- User claims pending rewards (moves to main balance). Amounts are negated.
CREATE OR REPLACE FUNCTION btracker_backend.process_claim_reward_balance_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
RETURN (
  WITH author_claim_reward_operation AS (
    SELECT
      ((_operation_body)->'value'->>'account')::TEXT AS account_name,
      - ((_operation_body)->'value'->'reward_hbd'->>'amount')::BIGINT AS claim_hbd,    -- negative: reduces pending
      - ((_operation_body)->'value'->'reward_hive'->>'amount')::BIGINT AS claim_hive,
      - ((_operation_body)->'value'->'reward_vests'->>'amount')::BIGINT AS claim_vests
  )
  SELECT
    (
      account_name,
      claim_hbd,
      claim_hive,
      claim_vests
    )::btracker_backend.impacted_rewards_return
  FROM author_claim_reward_operation
);

END
$$;

RESET ROLE;
