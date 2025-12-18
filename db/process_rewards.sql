SET ROLE btracker_owner;

/**
 * Processes reward operations (author, curation, benefactor, claim) for a block range.
 * Uses squashing pattern for fast sync and recursive CTE for claim balance clamping.
 */
CREATE OR REPLACE FUNCTION process_block_range_rewards(IN _from INT, IN _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- ============================================================================
  -- OPERATION TYPE IDs
  -- ============================================================================
  -- These are looked up dynamically from hafd.operation_types to avoid hardcoding.
  -- Each operation type has a unique ID that we use to dispatch to the correct
  -- processing function in the CASE statement below.
  _op_claim_reward_balance      INT := btracker_backend.op_claim_reward_balance();
  _op_author_reward             INT := btracker_backend.op_author_reward();
  _op_curation_reward           INT := btracker_backend.op_curation_reward();
  _op_comment_reward            INT := btracker_backend.op_comment_reward();
  _op_comment_benefactor_reward INT := btracker_backend.op_comment_benefactor_reward();

  -- ============================================================================
  -- NAI (Numeric Asset Identifier) TYPES
  -- ============================================================================
  -- NAIs are looked up ONCE at function start and stored in variables to avoid
  -- repeated function calls. This is critical for performance since these values
  -- are used in the final INSERT which may process thousands of rows.
  --
  -- NAI 13 (HBD):   Hive Backed Dollar - stablecoin pegged to USD
  -- NAI 21 (HIVE):  Native HIVE token
  -- NAI 37 (VESTS): Vesting shares representing staked HIVE
  -- NAI 38 (VESTS-as-HIVE): VIRTUAL type - NOT in asset_table!
  --                         Represents the HIVE equivalent value of VESTS.
  --                         Formula: vests * (total_vesting_fund_hive / total_vesting_shares)
  --                         Used for UI display so users see HIVE value instead of VESTS.
  _nai_hbd                      SMALLINT := btracker_backend.nai_hbd();
  _nai_hive                     SMALLINT := btracker_backend.nai_hive();
  _nai_vests                    SMALLINT := btracker_backend.nai_vests();
  _nai_vests_as_hive            SMALLINT := btracker_backend.nai_vests_as_hive();

  -- Counters for inserted rows (used for debugging/logging)
  __insert_rewards              INT;
  __insert_info_rewards         INT;
BEGIN

-- ============================================================================
-- CTE: ops
-- ============================================================================
-- WHY MATERIALIZED: Forces PostgreSQL to execute this query ONCE and cache results.
-- Without MATERIALIZED, the planner might inline this subquery into every downstream
-- CTE, causing redundant scans of operations_view (expensive with large block ranges).
--
-- SQUASHING PATTERN: During fast sync, we process large block ranges (e.g., 1000 blocks).
-- By fetching all operations for the range upfront, we "squash" many individual operations
-- into a single batch. This is orders of magnitude faster than processing block-by-block.
--
-- Example: Processing blocks 1-1000 in one call vs 1000 separate calls.
WITH ops AS MATERIALIZED (
  SELECT ov.body, ov.op_type_id, ov.id AS source_op, ov.block_num AS source_op_block
  FROM operations_view ov
  WHERE
    ov.op_type_id = ANY(ARRAY[_op_claim_reward_balance, _op_author_reward, _op_curation_reward, _op_comment_reward, _op_comment_benefactor_reward]) AND
    ov.block_num BETWEEN _from AND _to
),

-- ============================================================================
-- CTE: process_block_range_data_b
-- ============================================================================
-- WHY MATERIALIZED: This is the foundation for all downstream processing.
-- Materializing ensures the expensive operation fetch happens exactly once.
--
-- This CTE simply reshapes the ops data for consumption by LATERAL joins below.
-- It preserves all columns needed for both reward balance tracking (get_impacted_bal)
-- and info rewards tracking (get_impacted_posting, get_impacted_curation).
process_block_range_data_b AS MATERIALIZED (
  SELECT
    o.body,
    o.source_op,
    o.source_op_block,
    o.op_type_id
  FROM ops o
),

-- ============================================================================
-- CTE: blocks_data
-- ============================================================================
-- WHY MATERIALIZED: Prevents repeated joins to hive.blocks_view.
--
-- PURPOSE: Fetches the vesting ratio for VESTS->HIVE conversion at each block.
-- The ratio changes over time as more HIVE is vested or unvested.
--
-- VESTS-to-HIVE formula: vests * (total_vesting_fund_hive / total_vesting_shares)
--
-- Example: If total_vesting_fund_hive = 1,000,000 HIVE and total_vesting_shares = 2,000,000 VESTS
--          then 100 VESTS = 100 * (1M / 2M) = 50 HIVE equivalent
--
-- DISTINCT is used because multiple operations may occur in the same block,
-- but we only need one ratio per block number.
blocks_data AS MATERIALIZED (
  SELECT DISTINCT bv.num, bv.total_vesting_fund_hive, bv.total_vesting_shares
  FROM hive.blocks_view bv
  WHERE bv.num IN (SELECT DISTINCT source_op_block FROM process_block_range_data_b)
),

-- ============================================================================
-- CTE: get_impacted_bal
-- ============================================================================
-- WHY MATERIALIZED: The CASE + LATERAL pattern calls expensive JSON parsing functions.
-- Materializing ensures each operation is parsed exactly once.
--
-- LATERAL JOIN PATTERN: CROSS JOIN LATERAL allows us to call a function for each row
-- and expand the result into columns. The CASE statement routes each operation to
-- its appropriate processing function based on op_type_id.
--
-- REWARD TYPES:
--   - claim_reward_balance:    User claims accumulated rewards. Returns NEGATIVE amounts
--                              because claiming reduces the pending reward balance.
--   - author_reward:           Author receives HBD/HIVE/VESTS for creating content.
--   - comment_benefactor:      Beneficiary (set by author) receives share of rewards.
--
-- WHY payout_must_be_claimed CHECK: Some rewards are paid directly (payout_must_be_claimed=false)
-- and don't go through the pending reward balance. We only track pending rewards here.
-- Direct payouts are tracked separately in process_balances.sql.
get_impacted_bal AS MATERIALIZED (
  SELECT
    result.account_name,
    result.hbd_payout AS reward_hbd,
    result.hive_payout AS reward_hive,
    result.vesting_payout AS reward_vests,
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block
  FROM process_block_range_data_b fio
  CROSS JOIN LATERAL (
    -- Route to appropriate helper function based on operation type
    SELECT (
      CASE
        WHEN fio.op_type_id = _op_claim_reward_balance
          THEN btracker_backend.process_claim_reward_balance_operation(fio.body)
        WHEN fio.op_type_id = _op_author_reward
          THEN btracker_backend.process_author_reward_operation(fio.body, fio.source_op_block)
        WHEN fio.op_type_id = _op_comment_benefactor_reward
          THEN btracker_backend.process_benefactor_reward_operation(fio.body, fio.source_op_block)
      END
    ).*
  ) AS result
  WHERE
    -- Claims are always processed (reduce pending balance)
    fio.op_type_id = _op_claim_reward_balance OR
    -- Author/benefactor rewards only tracked if they go to pending balance
    (
      fio.op_type_id IN (_op_author_reward, _op_comment_benefactor_reward) AND
      (fio.body->'value'->>'payout_must_be_claimed')::BOOLEAN = true
    )
),

-- ============================================================================
-- CTE: get_impacted_posting
-- ============================================================================
-- PURPOSE: Track posting rewards for the account_info_rewards table.
-- This is separate from get_impacted_bal because info_rewards tracks TOTAL earned
-- rewards (for display purposes), not pending reward balance.
--
-- LATERAL pattern calls process_posting_rewards once per comment_reward_operation.
get_impacted_posting AS (
  SELECT
    result.account_name,
    result.reward AS posting_reward,
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block
  FROM process_block_range_data_b fio
  CROSS JOIN btracker_backend.process_posting_rewards(fio.body) AS result
  WHERE fio.op_type_id = _op_comment_reward
),

-- ============================================================================
-- CTE: get_impacted_curation
-- ============================================================================
-- PURPOSE: Track curation rewards (rewards for voting on content).
-- Curators receive VESTS for upvoting content before it becomes popular.
--
-- Note: payout_must_be_claimed is extracted here for later filtering in
-- union_operations_with_vesting_balance.
get_impacted_curation AS (
  SELECT
    result.account_name,
    result.reward,
    result.payout_must_be_claimed,
    fio.op_type_id,
    fio.source_op,
    fio.source_op_block
  FROM process_block_range_data_b fio
  CROSS JOIN btracker_backend.process_curation_rewards(fio.body) AS result
  WHERE fio.op_type_id = _op_curation_reward
),

-- ============================================================================
-- CTE: calculate_vests_from_curation
-- ============================================================================
-- WHY MATERIALIZED: This performs the VESTS->HIVE conversion which requires
-- joining with blocks_data. Materializing prevents repeated joins.
--
-- PURPOSE: Convert curation rewards from VESTS to HIVE equivalent (NAI 38).
-- Curation rewards are always paid in VESTS, but we display HIVE equivalent for users.
--
-- NULLIF prevents division by zero if total_vesting_shares is 0 (shouldn't happen in practice).
calculate_vests_from_curation AS MATERIALIZED (
  SELECT
    pc.account_name,
    pc.reward,
    (pc.reward * bd.total_vesting_fund_hive::NUMERIC / NULLIF(bd.total_vesting_shares, 0)::NUMERIC)::BIGINT AS curation_reward,
    pc.payout_must_be_claimed,
    pc.op_type_id,
    pc.source_op,
    pc.source_op_block
  FROM get_impacted_curation pc
  JOIN blocks_data bd ON bd.num = pc.source_op_block
),

-- ============================================================================
-- CTE: get_impacted_info_rewards
-- ============================================================================
-- PURPOSE: Combine posting and curation rewards for account_info_rewards table.
-- Uses UNION ALL because an account could have both posting AND curation rewards
-- in the same block range.
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

-- ============================================================================
-- CTE: group_by_account_info
-- ============================================================================
-- PURPOSE: Aggregate posting and curation rewards per account.
-- This is the final step before inserting into account_info_rewards.
group_by_account_info AS (
  SELECT
    account_name,
    SUM(posting_reward) AS posting,
    SUM(curation_reward) AS curation
  FROM get_impacted_info_rewards
  GROUP BY account_name
),

-- ============================================================================
-- CTE: calculate_vesting_balance
-- ============================================================================
-- PURPOSE: Calculate the HIVE-equivalent value (NAI 38) for VESTS in reward operations.
-- This is needed for non-claim operations (author/benefactor/curation rewards).
--
-- WHY EXCLUDE CLAIMS: Claim operations need special handling because they involve
-- PROPORTIONAL reduction of NAI 38 based on accumulated balance. This is handled
-- by the recursive CTE later. Here we set reward_vest_balance=0 for claims as a
-- placeholder that will be filled in by the recursive logic.
calculate_vesting_balance AS (
  SELECT
    account_name,
    reward_hbd,
    reward_hive,
    reward_vests,
    (reward_vests * bd.total_vesting_fund_hive::NUMERIC / NULLIF(bd.total_vesting_shares, 0)::NUMERIC)::BIGINT AS reward_vest_balance,
    op_type_id,
    source_op,
    source_op_block
  FROM get_impacted_bal
  JOIN blocks_data bd ON bd.num = source_op_block
  WHERE op_type_id != _op_claim_reward_balance
),

-- ============================================================================
-- CTE: union_operations_with_vesting_balance
-- ============================================================================
-- PURPOSE: Combine all operation types into a single stream for processing.
-- Each operation gets its NAI 38 value (HIVE equivalent of VESTS).
--
-- THREE SOURCES:
-- 1. Author/benefactor rewards from calculate_vesting_balance (NAI 38 calculated)
-- 2. Claim operations from get_impacted_bal (NAI 38 = 0, will be calculated recursively)
-- 3. Curation rewards (only if payout_must_be_claimed=true, meaning they go to pending balance)
union_operations_with_vesting_balance AS (
  -- Source 1: Author and benefactor rewards with VESTS->HIVE conversion already done
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

  -- Source 2: Claim operations (NAI 38 is 0 here - will be calculated by recursive CTE)
  -- Claims have NEGATIVE reward values because they reduce the pending balance
  SELECT
    account_name,
    reward_hbd,
    reward_hive,
    reward_vests,
    0 AS reward_vest_balance,  -- Placeholder: recursive CTE will calculate actual NAI 38
    op_type_id,
    source_op,
    source_op_block
  FROM get_impacted_bal
  WHERE op_type_id = _op_claim_reward_balance

  UNION ALL

  -- Source 3: Curation rewards that go to pending balance
  -- (payout_must_be_claimed=true means reward is pending, not directly paid)
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

-- ============================================================================
-- CTE: segmented_rows
-- ============================================================================
-- PURPOSE: Segment operations into groups for aggregation.
--
-- SEGMENTATION STRATEGY:
-- When an account has multiple operations in a block range, we need to handle
-- claims specially. Each claim with non-zero VESTS creates a new "segment".
--
-- Example sequence for account "alice":
--   Op 1: Author reward +1000 VESTS (segment 0)
--   Op 2: Author reward +500 VESTS  (segment 0)
--   Op 3: Claim -800 VESTS          (segment 1 starts here)
--   Op 4: Author reward +200 VESTS  (segment 1)
--
-- WHY: We can aggregate operations within a segment (sum the rewards), but
-- claims themselves need special handling because NAI 38 depends on the
-- ACCUMULATED balance at the time of claim.
segmented_rows AS (
  SELECT
    *,
    -- Increment segment counter each time we see a claim with non-zero VESTS
    -- Claims with 0 VESTS (e.g., claiming only HBD) don't affect NAI 38 calculation
    SUM(CASE WHEN op_type_id = _op_claim_reward_balance AND reward_vests != 0 THEN 1 ELSE 0 END)
      OVER (PARTITION BY account_name ORDER BY source_op) AS segment
  FROM union_operations_with_vesting_balance
),

-- ============================================================================
-- CTE: aggregated_rows
-- ============================================================================
-- PURPOSE: Sum rewards within each segment EXCLUDING the claim operation itself.
--
-- WHY EXCLUDE CLAIMS: The claim operation reduces the balance, but we need to
-- track the rewards that PRECEDE the claim separately. The claim itself will
-- be added back in union_aggregated_and_segmented_rows.
--
-- Example:
--   Segment 0: +1000 VESTS, +500 VESTS, Claim -800 VESTS
--   aggregated_rows for segment 0: reward_vests = 1500 (excludes the -800 claim)
aggregated_rows AS (
  SELECT
    account_name,
    -- Sum values excluding claims with non-zero VESTS (claims with zero VESTS ARE included)
    SUM(CASE WHEN op_type_id = _op_claim_reward_balance AND reward_vests != 0 THEN 0 ELSE reward_hbd END) AS reward_hbd,
    SUM(CASE WHEN op_type_id = _op_claim_reward_balance AND reward_vests != 0 THEN 0 ELSE reward_hive END) AS reward_hive,
    SUM(CASE WHEN op_type_id = _op_claim_reward_balance AND reward_vests != 0 THEN 0 ELSE reward_vests END) AS reward_vests,
    SUM(CASE WHEN op_type_id = _op_claim_reward_balance AND reward_vests != 0 THEN 0 ELSE reward_vest_balance END) AS reward_vest_balance,
    MAX(source_op) AS source_op,
    MAX(source_op_block) AS source_op_block
  FROM segmented_rows
  GROUP BY account_name, segment
),

-- ============================================================================
-- CTE: union_aggregated_and_segmented_rows
-- ============================================================================
-- WHY MATERIALIZED: This is a key intermediate result used by both the fast path
-- (non-claim accounts) and slow path (recursive CTE for claims). Materializing
-- prevents duplicate computation.
--
-- PURPOSE: Combine aggregated rewards with individual claim operations.
-- The is_claim flag distinguishes rewards (aggregated) from claims (individual).
union_aggregated_and_segmented_rows AS MATERIALIZED (
  -- Aggregated rewards (all non-claim operations summed by segment)
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
  -- Filter out empty segments (all zeros after aggregation)
  WHERE NOT (reward_hbd = 0 AND reward_hive = 0 AND reward_vests = 0 AND reward_vest_balance = 0)

  UNION ALL

  -- Individual claim operations (need special NAI 38 handling)
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
  WHERE op_type_id = _op_claim_reward_balance AND reward_vests != 0
  ORDER BY source_op
),

-- ============================================================================
-- OPTIMIZATION: TWO PROCESSING PATHS
-- ============================================================================
-- Accounts WITHOUT claims can be aggregated directly (fast path).
-- Accounts WITH claims need recursive CTE for NAI 38 accuracy (slow path).
-- This optimization avoids running the expensive recursive CTE for most accounts.

-- ============================================================================
-- CTE: accounts_with_claims
-- ============================================================================
-- PURPOSE: Identify which accounts have claim operations in this block range.
-- Used to split processing into fast path vs slow path.
accounts_with_claims AS (
  SELECT DISTINCT account_name
  FROM union_aggregated_and_segmented_rows
  WHERE is_claim
),

-- ============================================================================
-- CTE: non_claim_accounts_sum (FAST PATH)
-- ============================================================================
-- PURPOSE: Direct aggregation for accounts without claims.
-- Since there are no claims, NAI 38 can be simply summed - no proportional
-- reduction needed.
--
-- This is the FAST PATH - just sum everything without recursion.
-- Most accounts in a typical block range won't have claims.
non_claim_accounts_sum AS (
  SELECT
    account_name,
    SUM(reward_hbd)::BIGINT AS reward_hbd,
    SUM(reward_hive)::BIGINT AS reward_hive,
    SUM(reward_vests)::BIGINT AS reward_vests,
    SUM(reward_vest_balance)::BIGINT AS reward_vest_balance,
    MAX(source_op_block)::INT AS source_op_block,
    MAX(source_op)::BIGINT AS source_op
  FROM union_aggregated_and_segmented_rows
  WHERE account_name NOT IN (SELECT account_name FROM accounts_with_claims)
  GROUP BY account_name
),

-- ============================================================================
-- CTE: claim_accounts_data (SLOW PATH PREPARATION)
-- ============================================================================
-- PURPOSE: Filter to only accounts that have claims for recursive processing.
claim_accounts_data AS (
  SELECT *
  FROM union_aggregated_and_segmented_rows
  WHERE account_name IN (SELECT account_name FROM accounts_with_claims)
),

-- ============================================================================
-- CTE: find_accounts_using_claim
-- ============================================================================
-- PURPOSE: Get distinct list of accounts with claims for balance lookup.
find_accounts_using_claim AS (
  SELECT DISTINCT
    account_name
  FROM claim_accounts_data
  WHERE is_claim
),

-- ============================================================================
-- CTE: prepare_prev_balances
-- ============================================================================
-- PURPOSE: Fetch previous VESTS (NAI 37) and VESTS-as-HIVE (NAI 38) balances
-- from account_rewards table.
--
-- WHY NEEDED: When calculating how much NAI 38 to reduce during a claim,
-- we need to know the ACCUMULATED balance including previous block ranges.
-- The recursive CTE only handles operations WITHIN this block range.
prepare_prev_balances AS (
  SELECT
    fa.account_name,
    ar.account AS account_id,
    ar.nai,
    ar.balance
  FROM account_rewards ar
  JOIN accounts_view av ON av.id = ar.account
  JOIN find_accounts_using_claim fa ON fa.account_name = av.name
  WHERE ar.nai IN (_nai_vests_as_hive, _nai_vests)
),

-- ============================================================================
-- CTE: join_prev_balances
-- ============================================================================
-- PURPOSE: Join previous balances to claim account data.
-- prev_vests: Previous NAI 37 balance (used for proportional reduction)
-- prev_hive_vests: Previous NAI 38 balance (accumulated HIVE equivalent)
join_prev_balances AS (
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
  FROM claim_accounts_data cp
  LEFT JOIN prepare_prev_balances ppb ON ppb.account_name = cp.account_name AND cp.is_claim AND ppb.nai = _nai_vests
  LEFT JOIN prepare_prev_balances ppbs ON ppbs.account_name = cp.account_name AND ppbs.nai = _nai_vests_as_hive
),

-- ============================================================================
-- CTE: sum_prev_vests_without_current_row
-- ============================================================================
-- PURPOSE: Calculate running sum of VESTS for each row EXCLUDING the current row.
-- This gives us the accumulated VESTS balance AT THE TIME of each operation.
--
-- ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING:
-- Sums all previous rows but NOT the current row.
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

-- ============================================================================
-- CTE: sum_vests_add_row_number
-- ============================================================================
-- WHY MATERIALIZED: Used by recursive CTE which needs stable, numbered rows.
--
-- PURPOSE: Add row numbers for recursive iteration and calculate total prev_vests
-- (including both previous block ranges AND previous operations in this range).
sum_vests_add_row_number AS MATERIALIZED (
  SELECT
    cp.account_name,
    cp.reward_hbd,
    cp.reward_hive,
    cp.reward_vests,
    cp.reward_vest_balance,
    -- Total previous VESTS = already-stored balance + sum of previous ops in this range
    COALESCE(cp.sum_prev_vests,0) + cp.prev_vests AS prev_vests,
    cp.prev_hive_vests,
    cp.source_op,
    cp.source_op_block,
    cp.is_claim,
    -- Row number for recursive iteration (1, 2, 3, ...)
    ROW_NUMBER() OVER (PARTITION BY cp.account_name ORDER BY cp.source_op) AS row_num
  FROM sum_prev_vests_without_current_row cp
),

-- ============================================================================
-- RECURSIVE CTE: recursive_vests
-- ============================================================================
-- WHY RECURSIVE: When an account has claims, NAI 38 (HIVE equivalent) must be
-- calculated INCREMENTALLY because each claim affects the accumulated balance.
--
-- THE PROBLEM:
-- For rewards: NAI 38 = vests * (total_vesting_fund_hive / total_vesting_shares)
--              This is straightforward and already calculated.
--
-- For claims:  NAI 38 = -(accumulated_hive_vests * claimed_vests / accumulated_vests)
--              This requires knowing the ACCUMULATED NAI 38 from ALL previous operations.
--
-- WHY NOT WINDOW FUNCTION:
-- Window functions like SUM() OVER can't do this because each row's NAI 38
-- depends on the COMPUTED NAI 38 of previous rows (not just input values).
-- This is a recursive dependency that requires WITH RECURSIVE.
--
-- EXAMPLE:
-- Previous balance: 500 VESTS, 250 HIVE equivalent (NAI 38 = 250)
-- Row 1: Author reward +1000 VESTS -> NAI 38 = 500 HIVE (at block's vesting ratio)
--        Accumulated: 1500 VESTS, 750 HIVE equivalent
-- Row 2: Claim -600 VESTS -> NAI 38 = -(750 * 600/1500) = -300 HIVE
--        (Proportional reduction: claiming 40% of VESTS reduces NAI 38 by 40%)
--        Accumulated: 900 VESTS, 450 HIVE equivalent
-- Row 3: Author reward +200 VESTS -> NAI 38 = 100 HIVE
--        Accumulated: 1100 VESTS, 550 HIVE equivalent
--
-- Row 2's calculation requires Row 1's result (750 HIVE accumulated).
-- This is why window functions can't solve this.
recursive_vests AS (
  WITH RECURSIVE calculated_vests AS (
    -- BASE CASE: First operation for each account (row_num = 1)
    SELECT
      cp.account_name,
      cp.reward_hbd,
      cp.reward_hive,
      cp.reward_vests,
      CASE
          WHEN cp.is_claim THEN
              -- Claim: proportional reduction based on previous accumulated balance
              -- prev_hive_vests = accumulated NAI 38 from previous block ranges
              -- prev_vests = accumulated VESTS from previous block ranges
              -- reward_vests is NEGATIVE for claims
              - ROUND((cp.prev_hive_vests) * (- cp.reward_vests) / cp.prev_vests, 3)
          ELSE
              -- Reward: use the pre-calculated NAI 38 value
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

    -- RECURSIVE CASE: Each subsequent operation uses previous row's accumulated values
    SELECT
      next_cp.account_name,
      next_cp.reward_hbd,
      next_cp.reward_hive,
      next_cp.reward_vests,
      CASE
          WHEN next_cp.is_claim THEN
              -- Use ACCUMULATED NAI 38 from previous row (includes this range's ops)
              -- prev.reward_vest_balance = NAI 38 change from previous row
              -- prev.prev_hive_vests = NAI 38 accumulated before previous row
              -- Sum these to get total accumulated NAI 38 at time of this claim
              - ROUND((prev.reward_vest_balance + prev.prev_hive_vests) * (- next_cp.reward_vests) / next_cp.prev_vests, 3)
          ELSE
              -- Reward: use the pre-calculated NAI 38 value
              next_cp.reward_vest_balance
      END AS reward_vest_balance,
      next_cp.prev_vests,
      -- Pass accumulated NAI 38 to next iteration
      (prev.reward_vest_balance + prev.prev_hive_vests) AS prev_hive_vests,
      next_cp.source_op,
      next_cp.source_op_block,
      next_cp.row_num,
      next_cp.is_claim
    FROM calculated_vests prev
    -- Join next row by row_num + 1 (iterate through operations in order)
    JOIN sum_vests_add_row_number next_cp ON prev.account_name = next_cp.account_name AND next_cp.row_num = prev.row_num + 1
  )
  SELECT * FROM calculated_vests
),

-- ============================================================================
-- CTE: claim_accounts_sum (SLOW PATH RESULT)
-- ============================================================================
-- PURPOSE: Aggregate results from recursive CTE for accounts WITH claims.
-- At this point, all NAI 38 values have been correctly calculated by the
-- recursive logic, so we can simply sum them.
claim_accounts_sum AS (
  SELECT
    account_name,
    SUM(reward_hbd)::BIGINT AS reward_hbd,
    SUM(reward_hive)::BIGINT AS reward_hive,
    SUM(reward_vests)::BIGINT AS reward_vests,
    SUM(reward_vest_balance)::BIGINT AS reward_vest_balance,
    MAX(source_op_block)::INT AS source_op_block,
    MAX(source_op)::BIGINT AS source_op
  FROM recursive_vests
  GROUP BY account_name
),

-- ============================================================================
-- CTE: all_accounts_sum
-- ============================================================================
-- PURPOSE: Combine results from both processing paths (fast + slow).
all_accounts_sum AS (
  SELECT * FROM non_claim_accounts_sum
  UNION ALL
  SELECT * FROM claim_accounts_sum
),

-- ============================================================================
-- CTE: all_account_names
-- ============================================================================
-- PURPOSE: Collect all unique account names for batch ID lookup.
-- Uses UNION (not UNION ALL) to deduplicate since an account might appear
-- in both reward paths and info_rewards.
all_account_names AS (
  SELECT DISTINCT account_name FROM all_accounts_sum
  UNION
  SELECT DISTINCT account_name FROM group_by_account_info
),

-- ============================================================================
-- CTE: account_ids
-- ============================================================================
-- PURPOSE: Batch lookup of account IDs from names.
--
-- OPTIMIZATION: Converting names to IDs in a single batch query is much faster
-- than looking up each account individually during INSERT. The accounts_view
-- has an index on name, so this IN clause is efficient.
account_ids AS (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (SELECT account_name FROM all_account_names)
),

-- ============================================================================
-- CTE: sum_operations
-- ============================================================================
-- PURPOSE: Join account IDs to aggregated rewards for INSERT.
sum_operations AS (
  SELECT
    ai.account_id,
    a.reward_hbd,
    a.reward_hive,
    a.reward_vests,
    a.reward_vest_balance,
    a.source_op_block,
    a.source_op
  FROM all_accounts_sum a
  JOIN account_ids ai ON ai.account_name = a.account_name
),

-- ============================================================================
-- CTE: posting_and_curations_account_id
-- ============================================================================
-- PURPOSE: Join account IDs to info rewards for INSERT.
posting_and_curations_account_id AS (
  SELECT
    ai.account_id,
    gb.posting,
    gb.curation
  FROM group_by_account_info gb
  JOIN account_ids ai ON ai.account_name = gb.account_name
),

-- ============================================================================
-- CTE: convert_rewards
-- ============================================================================
-- PURPOSE: Convert wide columns (4 separate balance columns) into rows (1 row per NAI).
--
-- UNNEST PATTERN: unnest(ARRAY[...]) expands arrays into rows, allowing us to
-- convert columnar data to row-based format for INSERT.
--
-- INPUT (1 row):  account_id, reward_hbd, reward_hive, reward_vests, reward_vest_balance
-- OUTPUT (4 rows): account_id, NAI=13, balance=reward_hbd
--                  account_id, NAI=21, balance=reward_hive
--                  account_id, NAI=37, balance=reward_vests
--                  account_id, NAI=38, balance=reward_vest_balance
convert_rewards AS (
  SELECT
    so.account_id,
    -- Expand NAIs into rows using parallel unnest
    unnest(ARRAY[_nai_hbd, _nai_hive, _nai_vests, _nai_vests_as_hive]) AS nai,
    unnest(ARRAY[so.reward_hbd, so.reward_hive, so.reward_vests, so.reward_vest_balance]) AS balance,
    so.source_op,
    so.source_op_block
  FROM sum_operations so
),

-- ============================================================================
-- CTE: prepare_ops_before_insert
-- ============================================================================
-- PURPOSE: Filter out zero balances before INSERT.
-- No point inserting a row that says "balance changed by 0".
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

-- ============================================================================
-- CTE: insert_sum_of_rewards
-- ============================================================================
-- PURPOSE: INSERT or UPDATE reward balances in account_rewards table.
--
-- ON CONFLICT (UPSERT) PATTERN:
-- - If (account, nai) doesn't exist: INSERT new row
-- - If (account, nai) exists: UPDATE by ADDING the new balance to existing
--
-- WHY ADD (not replace): This is the SQUASHING pattern. During fast sync,
-- we process multiple block ranges that each contribute delta values.
-- The running total is maintained by summing deltas.
--
-- Example:
--   Block range 1-100:  INSERT (alice, NAI=37, balance=1000)
--   Block range 101-200: UPDATE SET balance = 1000 + 500 = 1500
--   Block range 201-300: UPDATE SET balance = 1500 + (-200) = 1300 (claim reduced it)
--
-- source_op is updated to the latest operation, providing an audit trail.
insert_sum_of_rewards AS (
  INSERT INTO account_rewards
    (account, nai, balance, source_op)
  SELECT
    po.account_id,
    po.nai,
    po.balance,
    po.source_op
  FROM prepare_ops_before_insert po
  -- pk_account_rewards is PRIMARY KEY (account, nai)
  ON CONFLICT ON CONSTRAINT pk_account_rewards
  DO UPDATE SET
      -- Add delta to existing balance (squashing pattern)
      balance = account_rewards.balance + EXCLUDED.balance,
      -- Track which operation last modified this row
      source_op = EXCLUDED.source_op
  RETURNING account AS new_updated_acccounts
),

-- ============================================================================
-- CTE: insert_sum_of_info_rewards
-- ============================================================================
-- PURPOSE: INSERT or UPDATE info rewards (posting/curation totals).
-- Same ON CONFLICT pattern as above.
--
-- INFO REWARDS vs REWARD BALANCES:
-- - account_rewards: Tracks PENDING reward balance (can be claimed)
-- - account_info_rewards: Tracks TOTAL earned rewards (for display/analytics)
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

-- ============================================================================
-- FINAL: Execute CTEs and capture row counts
-- ============================================================================
-- The SELECT INTO triggers execution of all CTEs and stores the row counts.
-- This is useful for debugging/logging to verify operations completed.
SELECT
  (SELECT count(*) FROM insert_sum_of_rewards) AS rewards,
  (SELECT count(*) FROM insert_sum_of_info_rewards) AS info_rewards
INTO __insert_rewards, __insert_info_rewards;

END
$$;

RESET ROLE;
