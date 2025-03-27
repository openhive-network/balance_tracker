SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_rewards(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  __insert_rewards INT;
  __insert_info_rewards INT;
BEGIN
WITH process_block_range_data_b AS MATERIALIZED
(
  SELECT 
    ov.body,
    ov.id AS source_op,
    ov.block_num as source_op_block,
    ov.op_type_id 
  FROM operations_view ov
  WHERE 
  ov.op_type_id = ANY(ARRAY[39,51,52,63,53]) AND 
  ov.block_num BETWEEN _from AND _to
),
get_impacted_bal AS (
  SELECT 
    get_impacted_reward_balances.account_name,
    get_impacted_reward_balances.hbd_payout AS reward_hbd,
    get_impacted_reward_balances.hive_payout AS reward_hive,  
    get_impacted_reward_balances.vesting_payout AS reward_vests,    
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block 
  FROM process_block_range_data_b fio
  CROSS JOIN btracker_backend.get_impacted_reward_balances(fio.body, fio.source_op_block, fio.op_type_id) AS get_impacted_reward_balances
  WHERE 
    fio.op_type_id = 39 OR
    (fio.op_type_id IN (51,63) AND (fio.body->'value'->>'payout_must_be_claimed')::BOOLEAN = true)
),
------------------------------------------------------------------------------
--prepare info rewards data
get_impacted_posting AS (
  SELECT 
    get_posting.account_name,
    get_posting.reward AS posting_reward,
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block 
  FROM process_block_range_data_b fio
  CROSS JOIN btracker_backend.process_posting_rewards(fio.body) AS get_posting
  WHERE fio.op_type_id = 53
),
get_impacted_curation AS (
  SELECT 
    get_curation.account_name,
    get_curation.reward,
    get_curation.payout_must_be_claimed,
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block 
  FROM process_block_range_data_b fio
  CROSS JOIN btracker_backend.process_curation_rewards(fio.body) AS get_curation
  WHERE fio.op_type_id = 52
),
------------------------------------------------------------------------------
-- calculate curation_reward into hive
calculate_vests_from_curation AS MATERIALIZED (
  SELECT 
    pc.account_name,
    pc.reward,
    (pc.reward * bv.total_vesting_fund_hive::NUMERIC / NULLIF(bv.total_vesting_shares, 0)::NUMERIC)::BIGINT AS curation_reward,
    pc.payout_must_be_claimed,
    pc.op_type_id,
    pc.source_op,
    pc.source_op_block 
  FROM get_impacted_curation pc
  JOIN hive.blocks_view bv ON bv.num = pc.source_op_block
),
------------------------------------------------------------------------------
get_impacted_info_rewards AS (
  SELECT 
    account_name,
    posting_reward,
    0 AS curation_reward
  FROM get_impacted_posting

  UNION ALL

  SELECT 
    account_name,
    0 AS posting_reward,
    curation_reward
  FROM calculate_vests_from_curation 
),
-- prepare info rewards data
group_by_account_info AS (
  SELECT 
    account_name,
    SUM(posting_reward) AS posting,
    SUM(curation_reward) AS curation
  FROM get_impacted_info_rewards
  GROUP BY account_name
),
posting_and_curations_account_id AS (
  SELECT 
	  (SELECT av.id FROM accounts_view av WHERE av.name = account_name) AS account_id,
    posting,
    curation
  FROM group_by_account_info 
),
------------------------------------------------------------------------------
--vesting balance must be calculated for all operations except claim operation (can't be aggregated)
calculate_vesting_balance AS (
  SELECT 
    account_name,
    reward_hbd,
    reward_hive,  
    reward_vests,  
    (reward_vests * bv.total_vesting_fund_hive::NUMERIC / NULLIF(bv.total_vesting_shares, 0)::NUMERIC)::BIGINT AS reward_vest_balance,
    op_type_id,
    source_op,
    source_op_block
  FROM get_impacted_bal
  -- we use hive view because of performance issues during live sync  
  JOIN hive.blocks_view bv ON bv.num = source_op_block
  WHERE op_type_id != 39
),

union_operations_with_vesting_balance AS (
  SELECT 
    account_name,
    reward_hbd,
    reward_hive,  
    reward_vests,  
    reward_vest_balance,
    op_type_id,
    source_op,
    source_op_block
  FROM calculate_vesting_balance

  UNION ALL

  SELECT 
    account_name,
    reward_hbd,
    reward_hive,  
    reward_vests,  
    0 AS reward_vest_balance,
    op_type_id,
    source_op,
    source_op_block
  FROM get_impacted_bal
  WHERE op_type_id = 39

  UNION ALL

  SELECT 
    account_name,
    0 AS reward_hbd,
    0 AS reward_hive,  
    reward AS reward_vests,  
    curation_reward AS reward_vest_balance,
    op_type_id,
    source_op,
    source_op_block
  FROM calculate_vests_from_curation
  WHERE payout_must_be_claimed

),
------------------------------------------------------------------------------
-- aggregate rewards for each account (CLAIMS WITH reward_vests = 0 INCLUDED)
-- Identify segments between op_type_id = 39
segmented_rows AS (
  SELECT 
    *,
    SUM(CASE WHEN op_type_id = 39 AND reward_vests != 0 THEN 1 ELSE 0 END) OVER (PARTITION BY account_name ORDER BY source_op) AS segment
  FROM union_operations_with_vesting_balance
),
-- Aggregate balances for segments ending with op_type_id = 39
aggregated_rows AS (
  SELECT 
    account_name,
    SUM(CASE WHEN op_type_id = 39 AND reward_vests != 0 THEN 0 ELSE reward_hbd END) AS reward_hbd,
    SUM(CASE WHEN op_type_id = 39 AND reward_vests != 0 THEN 0 ELSE reward_hive END) AS reward_hive,
    SUM(CASE WHEN op_type_id = 39 AND reward_vests != 0 THEN 0 ELSE reward_vests END) AS reward_vests,
    SUM(CASE WHEN op_type_id = 39 AND reward_vests != 0 THEN 0 ELSE reward_vest_balance END) AS reward_vest_balance,
    MAX(source_op) AS source_op,
    MAX(source_op_block) AS source_op_block
  FROM segmented_rows
  GROUP BY account_name, segment
),
-- Select the results including the op_type_id = 39 records
union_aggregated_and_segmented_rows AS (
  SELECT 
    account_name,
    reward_hbd,
    reward_hive,
    reward_vests,
    reward_vest_balance,
    source_op,
    source_op_block,
    FALSE AS is_claim
  FROM aggregated_rows 
  WHERE NOT (reward_hbd = 0 AND reward_hive = 0 AND reward_vests = 0 AND reward_vest_balance = 0)
  -- Include 39 operations in the results
  UNION ALL

  SELECT 
    account_name,
    reward_hbd,
    reward_hive,
    reward_vests,
    reward_vest_balance,
    source_op,
    source_op_block,
    TRUE AS is_claim
  FROM segmented_rows
  WHERE op_type_id = 39 AND reward_vests != 0
  ORDER BY source_op
),
------------------------------------------------------------------------------
-- prepare previous balances for each account
find_accounts_using_claim AS (
  SELECT 
    account_name,
    (SELECT av.id FROM accounts_view av WHERE av.name = account_name) AS account_id
  FROM union_aggregated_and_segmented_rows 
  WHERE is_claim
  GROUP BY account_name
),
prepare_prev_balances AS (
  SELECT
    fa.account_name,
    ar.account AS account_id,
    ar.nai,
    ar.balance
  FROM account_rewards ar
  JOIN find_accounts_using_claim fa ON fa.account_id = ar.account
  WHERE nai IN (38,37)
),
join_prev_balances AS  (
  SELECT 
    cp.account_name,
    cp.reward_hbd,
    cp.reward_hive,  
    cp.reward_vests,  
    cp.reward_vest_balance,
	  COALESCE(ppb.balance, 0) AS prev_vests,
	  COALESCE(ppbs.balance, 0)::NUMERIC AS prev_hive_vests,
    cp.source_op,
    cp.source_op_block,
    cp.is_claim
  FROM union_aggregated_and_segmented_rows cp
  LEFT JOIN prepare_prev_balances ppb ON ppb.account_name = cp.account_name AND cp.is_claim AND ppb.nai = 37
  LEFT JOIN prepare_prev_balances ppbs ON ppbs.account_name = cp.account_name AND ppbs.nai = 38 
),
------------------------------------------------------------------------------
sum_prev_vests_without_current_row AS (
  SELECT 
    cp.account_name,
    cp.reward_hbd,
    cp.reward_hive,  
    cp.reward_vests,  
    cp.reward_vest_balance,
	  SUM(cp.reward_vests) OVER (PARTITION BY cp.account_name ORDER BY cp.source_op ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS sum_prev_vests,
    cp.prev_vests,
	  cp.prev_hive_vests,
    cp.source_op,
    cp.source_op_block,
    cp.is_claim
  FROM join_prev_balances cp
),
sum_vests_add_row_number AS MATERIALIZED (
  SELECT 
    cp.account_name,
    cp.reward_hbd,
    cp.reward_hive,  
    cp.reward_vests,  
    cp.reward_vest_balance,
    COALESCE(cp.sum_prev_vests,0) + cp.prev_vests AS prev_vests,
	  cp.prev_hive_vests,
    cp.source_op,
    cp.source_op_block,
    cp.is_claim,
    ROW_NUMBER() OVER (PARTITION BY cp.account_name ORDER BY cp.source_op) AS row_num
  FROM sum_prev_vests_without_current_row cp
),
------------------------------------------------------------------------------
recursive_vests AS (
  WITH RECURSIVE calculated_vests AS (
    SELECT 
      cp.account_name,
      cp.reward_hbd,
      cp.reward_hive,
      cp.reward_vests,
      CASE 
          WHEN cp.is_claim THEN
              - ROUND((cp.prev_hive_vests) * (- cp.reward_vests) / cp.prev_vests, 3)
          ELSE 
              cp.reward_vest_balance
      END AS reward_vest_balance,
      cp.prev_vests,
      cp.prev_hive_vests,
      cp.source_op,
      cp.source_op_block,
      cp.row_num,
      cp.is_claim
    FROM sum_vests_add_row_number cp
    WHERE cp.row_num = 1

    UNION ALL

    SELECT 
      next_cp.account_name,
      next_cp.reward_hbd,
      next_cp.reward_hive,
      next_cp.reward_vests,
      CASE 
          WHEN next_cp.is_claim THEN
              - ROUND((prev.reward_vest_balance + prev.prev_hive_vests) * (- next_cp.reward_vests) / next_cp.prev_vests, 3)
          ELSE 
              next_cp.reward_vest_balance
      END AS reward_vest_balance,
      next_cp.prev_vests,
      (prev.reward_vest_balance + prev.prev_hive_vests) AS prev_hive_vests,
      next_cp.source_op,
      next_cp.source_op_block,
      next_cp.row_num,
      next_cp.is_claim
    FROM calculated_vests prev
    JOIN sum_vests_add_row_number next_cp ON prev.account_name = next_cp.account_name AND next_cp.row_num = prev.row_num + 1
  )
  SELECT * FROM calculated_vests
),
------------------------------------------------------------------------------
sum_operations AS (
	SELECT 
	  (SELECT av.id FROM accounts_view av WHERE av.name = account_name) AS account_id,
	  SUM(reward_hbd)::BIGINT AS reward_hbd,
	  SUM(reward_hive)::BIGINT AS reward_hive,
	  SUM(reward_vests)::BIGINT AS reward_vests,
	  SUM(reward_vest_balance)::BIGINT AS reward_vest_balance,
	  MAX(source_op_block)::INT AS source_op_block,
	  MAX(source_op)::BIGINT AS source_op
	FROM recursive_vests
	GROUP BY account_name
),
convert_rewards AS (
  SELECT 
    account_id, 
    unnest(ARRAY[13, 21, 37, 38]) AS nai,
    unnest(ARRAY[reward_hbd, reward_hive, reward_vests, reward_vest_balance]) AS balance,
    source_op,
    source_op_block
  FROM sum_operations
),
prepare_ops_before_insert AS (
  SELECT 
    account_id,
    nai,
    balance,
    source_op,
    source_op_block
  FROM convert_rewards
  WHERE balance != 0
),
------------------------------------------------------------------------------
insert_sum_of_rewards AS (
  INSERT INTO account_rewards
    (account, nai, balance, source_op, source_op_block) 
  SELECT
    po.account_id,
    po.nai,
    po.balance,
    po.source_op,
    po.source_op_block
  FROM prepare_ops_before_insert po
  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
      balance = account_rewards.balance + EXCLUDED.balance,
      source_op = EXCLUDED.source_op,
      source_op_block = EXCLUDED.source_op_block
  RETURNING account AS new_updated_acccounts
),
insert_sum_of_info_rewards AS (
  INSERT INTO account_info_rewards
    (account, posting_rewards, curation_rewards)
  SELECT
    gb.account_id,
    gb.posting,
    gb.curation
  FROM posting_and_curations_account_id gb
  ON CONFLICT ON CONSTRAINT pk_account_info_rewards
  DO UPDATE SET
      posting_rewards = account_info_rewards.posting_rewards + EXCLUDED.posting_rewards,
      curation_rewards = account_info_rewards.curation_rewards + EXCLUDED.curation_rewards
  RETURNING account AS new_updated_acccounts
)
------------------------------------------------------------------------------
SELECT
  (SELECT count(*) FROM insert_sum_of_rewards) AS rewards,
  (SELECT count(*) FROM insert_sum_of_info_rewards) AS info_rewards
INTO __insert_rewards, __insert_info_rewards;

END
$$;

RESET ROLE;
