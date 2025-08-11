-- noqa: disable=all

SET ROLE btracker_owner;

CREATE OR REPLACE VIEW current_account_balances_view AS
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

CREATE OR REPLACE VIEW account_balance_history_view AS
  SELECT
    t.account,
    t.nai,
    t.balance,
    t.balance_seq_no,
    t.source_op,
    hafd.operation_id_to_block_num( t.source_op ) AS source_op_block,
    hafd.operation_id_to_type_id( t.source_op ) AS op_type_id
  FROM account_balance_history t;


RESET ROLE;
