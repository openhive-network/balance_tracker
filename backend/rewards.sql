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

-- Process comment_reward_operation (posting rewards)
CREATE OR REPLACE FUNCTION btracker_backend.process_posting_rewards(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'author')::TEXT,
    ((_operation_body)->'value'->>'author_rewards')::BIGINT,
    TRUE
  )::btracker_backend.impacted_info_rewards_return;

END
$$;

-- Process curation_reward_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_curation_rewards(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'curator')::TEXT,
    ((_operation_body)->'value'->'reward'->>'amount')::BIGINT,
    ((_operation_body)->'value'->>'payout_must_be_claimed')::BOOLEAN
  )::btracker_backend.impacted_info_rewards_return;

END
$$;

-- Process author_reward_operation
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
  FROM author_reward_operation
);

END
$$;

-- Process comment_benefactor_reward_operation
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
      ((_operation_body)->'value'->>'benefactor')::TEXT AS account_name,
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

-- Process claim_reward_balance_operation
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
      - ((_operation_body)->'value'->'reward_hbd'->>'amount')::BIGINT AS claim_hbd,
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
