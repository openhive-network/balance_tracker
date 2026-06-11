-- noqa: disable=all

/**
 * Backend Views for Balance Tracker
 * ==================================
 *
 * This file defines helper views that add derived columns to base tables.
 * These views extract commonly-needed information from the source_op column
 * (operation ID) to avoid repeated function calls in query code.
 *
 * Why Views?
 * ----------
 * The base tables store only the source_op (operation ID) to save space.
 * The operation ID is a composite value that encodes:
 * - Block number (which block the operation is in)
 * - Position within block
 *
 * Extracting block numbers requires a HAF function. By defining views,
 * we compute these values once and reuse them across many queries.
 *
 * HAF Functions Used:
 * -------------------
 * - hafd.operation_id_to_block_num(op_id): Extracts block number from operation ID
 *
 * Derived Columns Added:
 * ----------------------
 * - source_op_block: Block number where the operation occurred
 *
 * Usage:
 * ------
 * Backend functions query these views instead of base tables when they need
 * block numbers. For pagination by block range, the source_op_block column
 * enables efficient filtering.
 *
 * View Naming Convention:
 * -----------------------
 * All views are named {base_table_name}_view and live in btracker_backend schema.
 *
 * Performance Note:
 * -----------------
 * These are simple views (not materialized), so the function calls happen
 * at query time. The HAF functions are optimized for this use case.
 *
 * @see db/btracker_app.sql for base table definitions
 */

SET ROLE btracker_owner;

/**
 * current_account_balances_view
 * -----------------------------
 * View over current_account_balances with extracted block info.
 *
 * Base table: current_account_balances
 * Purpose: Current liquid balances (HIVE, HBD, VESTS) for each account
 *
 * Columns:
 * - account: Account ID
 * - nai: Asset type (13=HBD, 21=HIVE, 37=VESTS)
 * - balance: Current balance amount
 * - balance_change_count: Total number of balance changes for this account/nai
 * - source_op: Operation ID of most recent balance change
 * - source_op_block: Block number of most recent balance change
 */
CREATE OR REPLACE VIEW btracker_backend.current_account_balances_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_change_count,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM current_account_balances t;

/**
 * account_balance_history_view
 * ----------------------------
 * View over account_balance_history with extracted block info.
 *
 * Base table: account_balance_history
 * Purpose: Complete history of all liquid balance changes
 *
 * Key columns:
 * - balance_seq_no: Sequence number for pagination (per account/nai)
 * - source_op_block: Block number (used for block range filtering)
 *
 * This is the primary view used by balance history endpoints.
 * The balance_seq_no enables efficient cursor-based pagination.
 */
CREATE OR REPLACE VIEW btracker_backend.account_balance_history_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_seq_no,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_balance_history t;

/**
 * account_savings_view
 * --------------------
 * View over account_savings with extracted block info.
 *
 * Base table: account_savings
 * Purpose: Current savings balances (HIVE and HBD only, not VESTS)
 *
 * Includes savings_withdraw_requests: Count of pending withdrawal requests.
 * Savings withdrawals have a 3-day delay on Hive.
 */
CREATE OR REPLACE VIEW btracker_backend.account_savings_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_change_count,
    t.source_op,
    t.savings_withdraw_requests,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_savings t;

/**
 * account_savings_history_view
 * ----------------------------
 * View over account_savings_history with extracted block info.
 *
 * Base table: account_savings_history
 * Purpose: Complete history of all savings balance changes
 *
 * Similar to account_balance_history_view but for savings accounts.
 * Used by bh_savings_balance() for pagination range calculations.
 */
CREATE OR REPLACE VIEW btracker_backend.account_savings_history_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_seq_no,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_savings_history t;

/**
 * account_rewards_view
 * --------------------
 * View over account_rewards with extracted block info.
 *
 * Base table: account_rewards
 * Purpose: Pending reward balances (unclaimed author/curation rewards)
 *
 * Tracks HIVE, HBD, and VESTS rewards that can be claimed via
 * claim_reward_balance operation.
 */
CREATE OR REPLACE VIEW btracker_backend.account_rewards_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_rewards t;

/**
 * current_accounts_delegations_view
 * ----------------------------------
 * View over current_accounts_delegations with extracted block info.
 *
 * Base table: current_accounts_delegations
 * Purpose: Active vesting share delegations between accounts
 *
 * Each row represents a delegation from delegator to delegatee.
 * Balance is the amount of VESTS delegated.
 * When balance becomes 0, the delegation is removed after 5-day return period.
 */
CREATE OR REPLACE VIEW btracker_backend.current_accounts_delegations_view AS
  SELECT
    t.delegator,
    t.delegatee,
    t.balance,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM current_accounts_delegations t;

/**
 * recurrent_transfers_view
 * ------------------------
 * View over recurrent_transfers with extracted block info.
 *
 * Base table: recurrent_transfers
 * Purpose: Active scheduled recurring transfers
 *
 * Columns:
 * - from_account/to_account: Sender and recipient
 * - transfer_id: Unique ID for this recurrent transfer
 * - amount/nai: Transfer amount and asset type
 * - remaining_executions: How many transfers left to execute
 * - consecutive_failures: Failure count (10 = auto-cancel)
 * - recurrence: Hours between executions
 * - memo: Optional transfer memo
 */
CREATE OR REPLACE VIEW btracker_backend.recurrent_transfers_view AS
  SELECT
    t.from_account,
    t.to_account,
    t.transfer_id,
    t.nai,
    t.amount,
    t.consecutive_failures,
    t.remaining_executions,
    t.recurrence,
    t.memo,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM recurrent_transfers t;

/**
 * account_withdraws_view
 * ----------------------
 * View over account_withdraws with extracted block info.
 *
 * Base table: account_withdraws
 * Purpose: Power-down (vesting withdrawal) state per account
 *
 * Columns:
 * - vesting_withdraw_rate: VESTS withdrawn per week
 * - to_withdraw: Total VESTS to withdraw over 13 weeks
 * - withdrawn: VESTS already withdrawn
 * - withdraw_routes: Number of routing rules (where vests go)
 * - delayed_vests: VESTS under 30-day voting delay (HF24+)
 */
CREATE OR REPLACE VIEW btracker_backend.account_withdraws_view AS
  SELECT
    t.account,
    t.vesting_withdraw_rate,
    t.to_withdraw,
    t.withdrawn,
    t.withdraw_routes,
    t.delayed_vests,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_withdraws t;

/**
 * account_routes_view
 * -------------------
 * View over account_routes with extracted block info.
 *
 * Base table: account_routes
 * Purpose: Vesting withdrawal routing rules
 *
 * When powering down, VESTS can be routed to other accounts.
 * Each route specifies a percentage (0-10000 = 0-100.00%) of
 * the weekly withdrawal to send to another account.
 */
CREATE OR REPLACE VIEW btracker_backend.account_routes_view AS
  SELECT
    t.account,
    t.to_account,
    t.percent,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM account_routes t;

/**
 * balance_history_by_month_view
 * -----------------------------
 * View over balance_history_by_month with extracted block info.
 *
 * Base table: balance_history_by_month
 * Purpose: Monthly aggregated balance snapshots
 *
 * Columns:
 * - updated_at: Month timestamp (truncated to month start)
 * - balance: Balance at end of month
 * - min_balance: Lowest balance during the month
 * - max_balance: Highest balance during the month
 *
 * Used for generating historical balance charts at monthly granularity.
 */
CREATE OR REPLACE VIEW btracker_backend.balance_history_by_month_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM balance_history_by_month t;

/**
 * balance_history_by_day_view
 * ---------------------------
 * View over balance_history_by_day with extracted block info.
 *
 * Base table: balance_history_by_day
 * Purpose: Daily aggregated balance snapshots
 *
 * Same structure as balance_history_by_month_view but at daily granularity.
 * Used for generating historical balance charts over shorter time periods.
 */
CREATE OR REPLACE VIEW btracker_backend.balance_history_by_day_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM balance_history_by_day t;

/**
 * saving_history_by_month_view
 * ----------------------------
 * View over saving_history_by_month with extracted block info.
 *
 * Base table: balance_history_by_month (note: currently using same source)
 * Purpose: Monthly aggregated savings balance snapshots
 *
 * Same structure as balance_history_by_month_view but for savings balances.
 * Note: VESTS are not valid for savings (only HIVE and HBD).
 */
CREATE OR REPLACE VIEW btracker_backend.saving_history_by_month_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM balance_history_by_month t;

/**
 * saving_history_by_day_view
 * --------------------------
 * View over saving_history_by_day with extracted block info.
 *
 * Base table: balance_history_by_day (note: currently using same source)
 * Purpose: Daily aggregated savings balance snapshots
 *
 * Same structure as balance_history_by_day_view but for savings balances.
 * Note: VESTS are not valid for savings (only HIVE and HBD).
 */
CREATE OR REPLACE VIEW btracker_backend.saving_history_by_day_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM balance_history_by_day t;

/**
 * account_hbd_interest_view
 * -------------------------
 * View over account_hbd_interest.
 *
 * Base table: account_hbd_interest
 * Purpose: Per-account liquid HBD interest accumulator for HAFBE get_account.
 *
 * A consumer (e.g. HAFBE get_account) reads this view by account to compute
 * pending HBD interest. The final interest amount is computed by the consumer
 * because it depends on the witness median HBD interest rate (top-20 ranked
 * witnesses), which is outside Balance Tracker's scope.
 *
 * IMPORTANT — the stored hbd_seconds is only accrued up to the last balance change.
 * A consumer must add the interest still pending for the idle period since then,
 * BEFORE applying the rate:
 *
 *     hbd_seconds_pending = hbd_seconds
 *                         + last_balance * EXTRACT(EPOCH FROM (now - hbd_seconds_last_update))
 *     interest = hbd_seconds_pending / SECONDS_PER_YEAR * interest_rate / 10000
 *
 * Using the raw hbd_seconds without the pending term undercounts accounts that
 * have not transacted since their last balance change.
 *
 * Columns:
 * - hbd_seconds: accumulated (balance × seconds) since last interest payment (NOT incl. idle period)
 * - hbd_seconds_last_update: timestamp of last liquid HBD balance change
 * - last_balance: liquid HBD balance at hbd_seconds_last_update
 * - hbd_last_interest_payment: timestamp of last liquid HBD interest payment
 *
 * No source_op column: this table stores timestamps directly from block data.
 */
CREATE OR REPLACE VIEW btracker_backend.account_hbd_interest_view AS
  SELECT
    t.account,
    t.hbd_seconds,
    t.hbd_seconds_last_update,
    t.last_balance,
    t.hbd_last_interest_payment
  FROM account_hbd_interest t;

/**
 * current_rc_delegations_view
 * ---------------------------
 * View over current_rc_delegations with extracted block info.
 *
 * Base table: current_rc_delegations
 * Purpose: Active RC (Resource Credit) delegations between accounts
 *
 * Each row represents an RC delegation from delegator to delegatee.
 * max_rc is the amount of Resource Credits delegated.
 * When max_rc becomes 0, the delegation is removed.
 *
 * Note: RC delegations were introduced in HF26.
 */
CREATE OR REPLACE VIEW btracker_backend.current_rc_delegations_view AS
  SELECT
    t.delegator,
    t.delegatee,
    t.max_rc,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block
  FROM current_rc_delegations t;


RESET ROLE;
