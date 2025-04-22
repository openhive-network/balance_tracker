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
  add_row_num AS (
    SELECT 
      bp.from_account,
      bp.to_account,
      bp.transfer_id,
      bp.nai,
      bp.amount,
      bp.consecutive_failures,
      bp.remaining_executions,
      bp.recurrence,
      bp.memo,
      bp.delete_transfer,
      bp.source_op,
      bp.source_op_block,
      bp.op_type_id,
      ROW_NUMBER() OVER (PARTITION BY bp.from_account, bp.to_account, bp.transfer_id, bp.op_type_id ORDER BY bp.source_op DESC) AS rn
    FROM process_block_range_data_b bp
  ),
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
    FROM add_row_num ar
    WHERE ar.rn = 1
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
        ed.source_op,
        ed.source_op_block,
        ed.rn_per_transfer_asc,
        ed.rn_per_transfer_desc
      FROM excluded_deleted_transfers ed
      WHERE ed.rn_per_transfer_asc = 1

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
        next_cp.source_op,
        next_cp.source_op_block,
        next_cp.rn_per_transfer_asc,
        next_cp.rn_per_transfer_desc
      FROM calculated_transfers prev
      JOIN excluded_deleted_transfers next_cp ON 
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
    WHERE nai IS NOT NULL
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
  -- update failures and remaining executions for transfers that were not deleted or replaced by another transfer
  update_transfers AS (
    UPDATE recurrent_transfers rt SET
      consecutive_failures = rvt.consecutive_failures,
      remaining_executions = rvt.remaining_executions,
      source_op = rvt.source_op,
      source_op_block = rvt.source_op_block
    FROM recursive_transfers rvt
    WHERE 
      rt.from_account = rvt.from_account_id AND
      rt.to_account = rvt.to_account_id AND
      rt.transfer_id = rvt.transfer_id AND
      rvt.nai IS NULL
    RETURNING rt.from_account AS updated_account_id
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
    (SELECT count(*) FROM update_transfers) AS update_transfers,
    (SELECT count(*) FROM delete_canceled_transfers) AS delete_canceled_transfers
  INTO __insert_transfers, __update_transfers, __delete_canceled_transfers;

END
$$;

RESET ROLE;
