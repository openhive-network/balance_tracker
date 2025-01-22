SET ROLE btracker_owner;

DROP TYPE IF EXISTS impacted_info_rewards_return CASCADE;
CREATE TYPE impacted_info_rewards_return AS
(
    account_name VARCHAR,
    posting_reward BIGINT,
    curation_reward BIGINT
);

DROP TYPE IF EXISTS impacted_rewards_return CASCADE;
CREATE TYPE impacted_rewards_return AS
(
    account_name VARCHAR,
    hbd_payout BIGINT,
    hive_payout BIGINT,
    vesting_payout BIGINT
);

CREATE OR REPLACE FUNCTION get_impacted_reward_balances(IN _operation_body JSONB, IN _source_op_block INT, IN _op_type_id INT)
RETURNS impacted_rewards_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 39 THEN
        process_claim_reward_balance_operation(_operation_body)

      WHEN _op_type_id = 51 OR _op_type_id = 63 THEN
        process_author_or_benefactor_reward_operation(_operation_body, _source_op_block, _op_type_id)

      WHEN _op_type_id = 52 THEN
        process_curation_reward_operation(_operation_body, _source_op_block)
    END
  );

END;
$BODY$;


CREATE OR REPLACE FUNCTION get_impacted_info_reward_balances(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS impacted_info_rewards_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 52 THEN
        process_curation_rewards(_operation_body)

      WHEN _op_type_id = 53 THEN
        process_posting_rewards(_operation_body)
    END
  );

END;
$BODY$;

CREATE OR REPLACE FUNCTION process_posting_rewards(IN _operation_body JSONB)
RETURNS impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'author')::TEXT,
    ((_operation_body)->'value'->>'author_rewards')::BIGINT,
    0
  )::impacted_info_rewards_return;
  
END
$$;

CREATE OR REPLACE FUNCTION process_curation_rewards(IN _operation_body JSONB)
RETURNS impacted_info_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'curator')::TEXT,
    0,
    ((_operation_body)->'value'->'reward'->>'amount')::BIGINT

  )::impacted_info_rewards_return;
  
END
$$;

CREATE OR REPLACE FUNCTION process_author_or_benefactor_reward_operation(IN _operation_body JSONB, IN _source_op_block INT, IN _op_type_id INT)
RETURNS impacted_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
RETURN (
  WITH author_reward_operation AS (
    SELECT
      (CASE 
        WHEN _op_type_id = 51 THEN
          (_operation_body)->'value'->>'author'
        ELSE
          (_operation_body)->'value'->>'benefactor'
        END
      )::TEXT AS account_name,
      ((_operation_body)->'value'->'hbd_payout'->>'amount')::BIGINT AS hbd_payout,
      ((_operation_body)->'value'->'hive_payout'->>'amount')::BIGINT AS hive_payout,
      ((_operation_body)->'value'->'vesting_payout'->>'amount')::BIGINT AS vesting_payout
  ),
  add_vesting_balance AS (
    SELECT 
      account_name, 
      hbd_payout,
      hive_payout,
      vesting_payout
    FROM author_reward_operation
  )
  SELECT 
    (
      account_name, 
      hbd_payout,
      hive_payout,
      vesting_payout
    )::impacted_rewards_return
  FROM add_vesting_balance
);

END
$$;

CREATE OR REPLACE FUNCTION process_curation_reward_operation(IN _operation_body JSONB, IN _source_op_block INT)
RETURNS impacted_rewards_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
RETURN (
  WITH author_curation_operation AS (
    SELECT
      ((_operation_body)->'value'->>'curator')::TEXT AS account_name,
      0 AS hbd_payout,
      0 AS hive_payout,
      ((_operation_body)->'value'->'reward'->>'amount')::BIGINT AS vesting_payout
  )  
  SELECT 
    (
      account_name, 
      hbd_payout,
      hive_payout,
      vesting_payout
    )::impacted_rewards_return
  FROM author_curation_operation
);
  
END
$$;

CREATE OR REPLACE FUNCTION process_claim_reward_balance_operation(IN _operation_body JSONB)
RETURNS impacted_rewards_return
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
    )::impacted_rewards_return
  FROM author_claim_reward_operation
);

END
$$;

RESET ROLE;
