-- noqa: disable=all

SET ROLE btracker_owner;

CREATE OR REPLACE VIEW btracker_backend.current_account_balances_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_change_count,
    t.source_op,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM current_account_balances t;

CREATE OR REPLACE VIEW btracker_backend.account_balance_history_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_seq_no,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_balance_history t;

CREATE OR REPLACE VIEW btracker_backend.account_savings_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_change_count,
    t.source_op,
    t.savings_withdraw_requests,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_savings t;

CREATE OR REPLACE VIEW btracker_backend.account_savings_history_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_seq_no,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_savings_history t;

CREATE OR REPLACE VIEW btracker_backend.account_rewards_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.source_op,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_rewards t;

CREATE OR REPLACE VIEW btracker_backend.current_accounts_delegations_view AS
  SELECT
    t.delegator,
    t.delegatee,
    t.balance,
    t.source_op,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM current_accounts_delegations t;

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
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM recurrent_transfers t;

CREATE OR REPLACE VIEW btracker_backend.account_withdraws_view AS
  SELECT
    t.account,
    t.vesting_withdraw_rate,
    t.to_withdraw,
    t.withdrawn,
    t.withdraw_routes,
    t.delayed_vests,
    t.source_op,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_withdraws t;

CREATE OR REPLACE VIEW btracker_backend.account_routes_view AS
  SELECT
    t.account,
    t.to_account,
    t.percent,
    t.source_op,
    -- since extra column was removed, extract the block number from the operation ID
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_routes t;

CREATE OR REPLACE VIEW btracker_backend.balance_history_by_month_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM balance_history_by_month t;

CREATE OR REPLACE VIEW btracker_backend.balance_history_by_day_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM balance_history_by_day t;

CREATE OR REPLACE VIEW btracker_backend.saving_history_by_month_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM balance_history_by_month t;

CREATE OR REPLACE VIEW btracker_backend.saving_history_by_day_view AS
  SELECT
    t.account,
    t.nai,
    t.updated_at,

    t.balance,
    t.min_balance,
    t.max_balance,

    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM balance_history_by_day t;


RESET ROLE;
