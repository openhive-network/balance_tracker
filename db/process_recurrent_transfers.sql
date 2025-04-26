SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_recurrent_transfers(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  __insert_transfers INT;
  __update_transfers INT;
  __delete_canceled_transfers INT;
BEGIN
  WITH process_block_range_data_b AS (
    SELECT 
      rto.from_account AS from_account,
      rto.to_account AS to_account,
      rto.transfer_id AS transfer_id,
      rto.nai AS nai,
      rto.amount AS amount,
      rto.consecutive_failures AS consecutive_failures,
      rto.remaining_executions AS remaining_executions,
      rto.recurrence AS recurrence,
      rto.memo AS memo,
      rto.delete_transfer AS delete_transfer,
      ov.id AS source_op,
      ov.block_num as source_op_block,
      ov.op_type_id 
    FROM operations_view ov
    CROSS JOIN btracker_backend.get_recurrent_transfer_operations(ov.body, ov.op_type_id) AS rto
    WHERE 
      ov.op_type_id IN (49,83,84) AND 
      ov.block_num BETWEEN _from AND _to
  ),
  ---------------------------------------------------------------------------------------
  latest_rec_transfers AS (
    SELECT 
      ar.from_account,
      ar.to_account,
      ar.transfer_id,
      ar.nai,
      ar.amount,
      ar.consecutive_failures,
      ar.remaining_executions,
      ar.recurrence,
      ar.memo,
      ar.delete_transfer,
      ar.source_op,
      ar.source_op_block,
      ar.op_type_id,
      ROW_NUMBER() OVER (PARTITION BY ar.from_account, ar.to_account, ar.transfer_id ORDER BY ar.source_op DESC) AS rn_per_transfer_desc
    FROM process_block_range_data_b ar
  ),
  delete_transfer AS MATERIALIZED (
    SELECT 
      lrt.from_account,
      lrt.to_account,
      (SELECT av.id FROM hive.accounts_view av WHERE av.name = lrt.from_account) AS from_account_id,
      (SELECT av.id FROM hive.accounts_view av WHERE av.name = lrt.to_account) AS to_account_id,
      lrt.transfer_id
    FROM latest_rec_transfers lrt
    WHERE lrt.delete_transfer = TRUE AND lrt.rn_per_transfer_desc = 1
  ),
  -- exclude records related to deleted transfers
  excluded_deleted_transfers AS MATERIALIZED (
    SELECT 
      lrt.from_account,
      lrt.to_account,
      (SELECT av.id FROM hive.accounts_view av WHERE av.name = lrt.from_account) AS from_account_id,
      (SELECT av.id FROM hive.accounts_view av WHERE av.name = lrt.to_account) AS to_account_id,
      lrt.transfer_id,
      lrt.nai,
      lrt.amount,
      lrt.consecutive_failures,
      lrt.remaining_executions,
      lrt.recurrence,
      lrt.memo,
      lrt.delete_transfer,
      lrt.source_op,
      lrt.source_op_block,
      lrt.op_type_id,
      lrt.rn_per_transfer_desc,
      ROW_NUMBER() OVER (PARTITION BY lrt.from_account, lrt.to_account, lrt.transfer_id ORDER BY lrt.source_op) AS rn_per_transfer_asc
    FROM latest_rec_transfers lrt
    WHERE NOT EXISTS (
      SELECT 1 
      FROM delete_transfer dt 
      WHERE 
        dt.from_account = lrt.from_account AND 
        dt.to_account   = lrt.to_account AND 
        dt.transfer_id  = lrt.transfer_id
    )
  ),
  latest_transfers_in_table AS (
    SELECT 
      edt.from_account_id,
      edt.to_account_id,
      edt.transfer_id,
      rt.nai,
      rt.amount,
      rt.consecutive_failures,
      rt.remaining_executions,
      rt.recurrence,
      rt.memo,
      FALSE AS delete_transfer,
      rt.source_op,
      rt.source_op_block,
      49 AS op_type_id,
      0 as rn_per_transfer_desc,
      0 AS rn_per_transfer_asc
    FROM excluded_deleted_transfers edt
    LEFT JOIN recurrent_transfers rt ON 
      rt.from_account = edt.from_account_id AND 
      rt.to_account = edt.to_account_id AND 
      rt.transfer_id = edt.transfer_id
    WHERE edt.rn_per_transfer_desc = 1
  ),
  union_total_transfers AS MATERIALIZED (
    SELECT
      ltin.from_account_id,
      ltin.to_account_id,
      ltin.transfer_id,
      ltin.nai,
      ltin.amount,
      ltin.consecutive_failures,
      ltin.remaining_executions,
      ltin.recurrence,
      ltin.memo,
      ltin.delete_transfer,
      ltin.source_op,
      ltin.source_op_block,
      ltin.op_type_id,
      ltin.rn_per_transfer_desc,
      ltin.rn_per_transfer_asc
    FROM latest_transfers_in_table ltin

    UNION ALL

    SELECT 
      edt.from_account_id,
      edt.to_account_id,
      edt.transfer_id,
      edt.nai,
      edt.amount,
      edt.consecutive_failures,
      edt.remaining_executions,
      edt.recurrence,
      edt.memo,
      edt.delete_transfer,
      edt.source_op,
      edt.source_op_block,
      edt.op_type_id,
      edt.rn_per_transfer_desc,
      edt.rn_per_transfer_asc
    FROM excluded_deleted_transfers edt
  ),
  recursive_transfers AS MATERIALIZED (
    WITH RECURSIVE calculated_transfers AS (
      SELECT 
        ed.from_account_id,
        ed.to_account_id,
        ed.transfer_id,
        ed.nai,
        ed.amount,
        ed.consecutive_failures,
        ed.remaining_executions,
        ed.recurrence,
        ed.memo,
        ed.delete_transfer,
        ed.source_op,
        ed.source_op_block,
        ed.rn_per_transfer_asc,
        ed.rn_per_transfer_desc
      FROM union_total_transfers ed
      WHERE ed.rn_per_transfer_asc = 0

      UNION ALL

      SELECT 
        prev.from_account_id,
        prev.to_account_id,
        prev.transfer_id,
        (CASE WHEN next_cp.op_type_id = 49 THEN next_cp.nai ELSE prev.nai END) AS nai,
        (CASE WHEN next_cp.op_type_id = 49 THEN next_cp.amount ELSE prev.amount END) AS amount,
        next_cp.consecutive_failures,
        next_cp.remaining_executions,
        (CASE WHEN next_cp.op_type_id = 49 THEN next_cp.recurrence ELSE prev.recurrence END) AS recurrence,
        (CASE WHEN next_cp.op_type_id = 49 THEN next_cp.memo ELSE prev.memo END) AS memo,
        next_cp.delete_transfer,
        next_cp.source_op,
        -- if the transfer is an update of previous transfer
        -- and between them there is no deletion of the transfer
        -- the block number that is used to update trigger date must not be changed
        -- so we need to use the block number from the previous transfer
        (
          CASE 
            WHEN next_cp.op_type_id = 49 AND next_cp.recurrence = prev.recurrence AND NOT prev.delete_transfer THEN 
              prev.source_op_block 
            ELSE 
              next_cp.source_op_block 
          END
        ) AS source_op_block,
        next_cp.rn_per_transfer_asc,
        next_cp.rn_per_transfer_desc
      FROM calculated_transfers prev
      JOIN union_total_transfers next_cp ON 
        prev.from_account_id          = next_cp.from_account_id AND 
        prev.to_account_id            = next_cp.to_account_id AND 
        prev.transfer_id              = next_cp.transfer_id AND 
        next_cp.rn_per_transfer_asc   = prev.rn_per_transfer_asc + 1
    )
    SELECT * FROM calculated_transfers
    WHERE rn_per_transfer_desc = 1
  ),
  ---------------------------------------------------------------------------------------
  -- insert or upsert transfers
  insert_transfers AS (
    INSERT INTO recurrent_transfers AS rt
      (
        from_account,
        to_account,
        transfer_id,
        nai,
        amount,
        consecutive_failures,
        remaining_executions,
        recurrence,
        memo,
        source_op,
        source_op_block
      )
    SELECT 
      from_account_id,
      to_account_id,
      transfer_id,
      nai,
      amount,
      consecutive_failures,
      remaining_executions,
      recurrence,
      memo,
      source_op,
      source_op_block
    FROM recursive_transfers
    ON CONFLICT ON CONSTRAINT pk_recurrent_transfers
    DO UPDATE SET
        nai = EXCLUDED.nai,
        amount = EXCLUDED.amount,
        consecutive_failures = EXCLUDED.consecutive_failures,
        remaining_executions = EXCLUDED.remaining_executions,
        recurrence = EXCLUDED.recurrence,
        memo = EXCLUDED.memo,
        source_op = EXCLUDED.source_op,
        source_op_block = EXCLUDED.source_op_block
    RETURNING rt.from_account AS from_account
  ),
  ---------------------------------------------------------------------------------------
  -- delete transfers if amount is 0 and remaining executions is 0 or consecutive failures is 10
  delete_canceled_transfers AS (
    DELETE FROM recurrent_transfers rt
    USING delete_transfer dt
    WHERE 
      rt.from_account = dt.from_account_id AND 
      rt.to_account = dt.to_account_id AND 
      rt.transfer_id = dt.transfer_id
    RETURNING rt.transfer_id AS transfer_id
  )

  SELECT
    (SELECT count(*) FROM insert_transfers) AS insert_transfers,
    (SELECT count(*) FROM delete_canceled_transfers) AS delete_canceled_transfers
  INTO __insert_transfers, __update_transfers, __delete_canceled_transfers;

END
$$;

RESET ROLE;
