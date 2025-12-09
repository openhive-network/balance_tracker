SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_escrows(
    IN _from INT,
    IN _to   INT,
    IN _report_step INT DEFAULT 1000
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$func$
DECLARE
  __op_transfer INT := (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_transfer_operation')::INT;
  __op_release  INT := (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_release_operation' )::INT;
  __op_approved INT := (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_approved_operation')::INT;
  __op_rejected INT := (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_rejected_operation')::INT;
  __op_dispute  INT := (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_dispute_operation')::INT;
  __ins_escrows INT := 0;
  __del_escrows INT := 0;
  __ins_fee     INT := 0;
  __del_fee     INT := 0;
BEGIN
  /* Process escrow operations in the specified block range
    - escrow_transfer_operation - creates an escrow and fee
    - escrow_approved_operation - deletes the fee, sets to_approved = TRUE
    - escrow_dispute_operation  - sets disputed = TRUE
    - escrow_release_operation  - deletes or reduces the escrow amounts
    - escrow_rejected_operation - deletes the escrow completely
  */

  WITH ops_in_range AS (
    SELECT ov.id AS op_id, ov.op_type_id, ov.body
    FROM operations_view ov
    WHERE
      ov.block_num BETWEEN _from AND _to AND
      ov.op_type_id IN (__op_transfer, __op_release, __op_approved, __op_rejected, __op_dispute)
  ),
  --------------------- GET ESCROW EVENTS ---------------------
  escrow_transfers AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = e.from_name) AS from_id,
      e.escrow_id,
      e.hive_nai,
      e.hive_amount,
      e.hbd_nai,
      e.hbd_amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_transfers(o.body) AS e
    WHERE o.op_type_id = __op_transfer
  ),
  escrow_releases AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = e.from_name) AS from_id,
      e.escrow_id,
      e.hive_nai,
      e.hive_amount,
      e.hbd_nai,
      e.hbd_amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_release(o.body) AS e
    WHERE o.op_type_id = __op_release
  ),
  escrow_fees AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = e.from_name) AS from_id,
      e.escrow_id,
      e.nai,
      e.amount
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_fees(o.body) AS e
    WHERE o.op_type_id = __op_transfer
  ),
  escrow_rejects AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = e.from_name) AS from_id,
      e.escrow_id
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_rejects (o.body) AS e
    WHERE o.op_type_id = __op_rejected
  ),
  escrow_approved AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = e.from_name) AS from_id,
      e.escrow_id
    FROM ops_in_range o
    CROSS JOIN btracker_backend.get_escrow_approves(o.body) AS e
    WHERE o.op_type_id = __op_approved
  ),
  escrow_disputes AS (
    SELECT
      o.op_id,
      (SELECT av.id FROM accounts_view av WHERE av.name = (o.body->'value'->>'from')::TEXT) AS from_id,
      (o.body->'value'->>'escrow_id')::BIGINT AS escrow_id
    FROM ops_in_range o
    WHERE o.op_type_id = __op_dispute
  ),
  --------------------- UNIQUE DISPUTES ---------------------
  unique_escrow_disputes AS MATERIALIZED (
    SELECT DISTINCT ON (ed.from_id, ed.escrow_id)
      ed.from_id,
      ed.escrow_id,
      ed.op_id
    FROM escrow_disputes ed
    ORDER BY ed.from_id, ed.escrow_id, ed.op_id DESC
  ),
  --------------------- UNIQUE FEES ---------------------
  unique_escrow_fees AS MATERIALIZED (
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.nai,
      er.amount,
      er.op_id
    FROM escrow_fees er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- UNIQUE REJECTS ---------------------
  unique_escrow_rejects AS MATERIALIZED (
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM escrow_rejects er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- UNIQUE APPROVES ---------------------
  unique_escrow_approves AS MATERIALIZED (
    SELECT DISTINCT ON (er.from_id, er.escrow_id)
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM escrow_approved er
    ORDER BY er.from_id, er.escrow_id, er.op_id DESC
  ),
  --------------------- TRANSFERS CREATED IN-RANGE ---------------------
  -- you can not have multiple transfers for the same (from_id, escrow_id) if transfer was not previously cancelled or completed
  -- we can assume that the latest transfer is the valid one
  create_transfers_in_range AS MATERIALIZED (
    SELECT DISTINCT ON (from_id, escrow_id)
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM escrow_transfers
    -- latest transfer per (from_id, escrow_id, nai)
    ORDER BY from_id, escrow_id, op_id DESC
  ),
  -- filter out transfers that were later rejected or completed
  filter_out_rejected_transfers AS MATERIALIZED (
    SELECT
      ct.*
    FROM create_transfers_in_range ct
    WHERE NOT EXISTS (
      SELECT 1
      FROM unique_escrow_rejects er
      WHERE
        er.from_id = ct.from_id AND
        er.escrow_id = ct.escrow_id AND
        er.op_id > ct.op_id
    )
  ),
  releases_related_to_in_range_transfers AS (
    SELECT
      er.op_id,
      er.from_id,
      er.escrow_id,
      er.hive_nai,
      er.hive_amount,
      er.hbd_nai,
      er.hbd_amount
    FROM escrow_releases er
    WHERE EXISTS (
      SELECT 1
      FROM filter_out_rejected_transfers frt
      WHERE
        frt.from_id = er.from_id AND
        frt.escrow_id = er.escrow_id AND
        er.op_id > frt.op_id
    )
  ),
  -- calculate final amounts after releases
  union_in_range_transfers_with_related_releases AS (
    SELECT
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM filter_out_rejected_transfers
    UNION ALL
    SELECT
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM releases_related_to_in_range_transfers
  ),
  aggregate_in_range_transfers AS (
    SELECT
      uit.from_id,
      uit.escrow_id,
      MAX(uit.hive_nai) AS hive_nai,
      SUM(uit.hive_amount) AS hive_amount,
      MAX(uit.hbd_nai) AS hbd_nai,
      SUM(uit.hbd_amount) AS hbd_amount,
      MIN(uit.op_id) AS op_id -- earliest op_id among the group is transfer creation
    FROM union_in_range_transfers_with_related_releases uit
    GROUP BY uit.from_id, uit.escrow_id
  ),
  filter_out_released_transfers AS (
    SELECT
      ait.*
    FROM aggregate_in_range_transfers ait
    WHERE NOT (ait.hive_amount = 0 AND ait.hbd_amount = 0)
  ),
  --------------------- PREPARE REJECTIONS ---------------------
  -- rejections in the range that relate to transfers created before the range
  rejections_to_pre_range_transfers AS (
    SELECT
      er.from_id,
      er.escrow_id,
      er.op_id
    FROM unique_escrow_rejects er
    -- exclude rejections that relate to transfers created in the range
    WHERE NOT EXISTS (
      SELECT 1
      FROM create_transfers_in_range frt
      WHERE
        frt.from_id = er.from_id AND
        frt.escrow_id = er.escrow_id AND
        frt.op_id > er.op_id
    )
  ),
  --------------------- RELEASES PRE-RANGE CREATED TRANSFERS---------------------
  releases_to_pre_range_transfers AS (
    SELECT
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      op_id
    FROM escrow_releases er
    -- only those releases that relate to transfers created before the range (not in create_transfers_in_range)
    WHERE NOT EXISTS (
      SELECT 1
      FROM create_transfers_in_range frt
      WHERE frt.from_id = er.from_id AND frt.escrow_id = er.escrow_id
    )
  ),
  filter_out_rejected_releases AS (
    SELECT
      rt.*
    FROM releases_to_pre_range_transfers rt
    WHERE NOT EXISTS (
      SELECT 1
      FROM unique_escrow_rejects er
      WHERE
        er.from_id = rt.from_id AND
        er.escrow_id = rt.escrow_id AND
        er.op_id > rt.op_id
    )
  ),
  aggregate_releases_to_pre_range_transfers AS (
    SELECT
      frt.from_id,
      frt.escrow_id,
      SUM(frt.hive_amount) AS hive_amount,
      SUM(frt.hbd_amount) AS hbd_amount
    FROM filter_out_rejected_releases frt
    GROUP BY frt.from_id, frt.escrow_id
  ),
  join_escrow_state_pre_range AS MATERIALIZED (
    SELECT
      ar.from_id,
      ar.escrow_id,
      es.hive_nai,
      (es.hive_amount + ar.hive_amount) AS hive_amount,
      es.hbd_nai,
      (es.hbd_amount + ar.hbd_amount) AS hbd_amount,
      es.source_op AS op_id -- keep first op_id from escrow_state
    FROM aggregate_releases_to_pre_range_transfers ar
    JOIN escrow_state es ON
      es.from_id = ar.from_id AND
      es.escrow_id = ar.escrow_id
  ),
  --------------------- PREPARE ESCROW DELETIONS ---------------------
  union_deletions AS MATERIALIZED (
    SELECT
      urr.from_id,
      urr.escrow_id,
      urr.op_id
    FROM rejections_to_pre_range_transfers urr
    UNION ALL
    SELECT
      jesp.from_id,
      jesp.escrow_id,
      jesp.op_id
    FROM join_escrow_state_pre_range jesp
    WHERE jesp.hive_amount = 0 AND jesp.hbd_amount = 0
  ),
  --------------------- PREPARE ESCROW INSERTS ---------------------
  union_inserts AS (
    SELECT
      frit.from_id,
      frit.escrow_id,
      frit.hive_nai,
      frit.hive_amount,
      frit.hbd_nai,
      frit.hbd_amount,
      frit.op_id
    FROM filter_out_released_transfers frit
    UNION ALL
    SELECT
      jesp.from_id,
      jesp.escrow_id,
      jesp.hive_nai,
      jesp.hive_amount,
      jesp.hbd_nai,
      jesp.hbd_amount,
      jesp.op_id
    FROM join_escrow_state_pre_range jesp
    WHERE jesp.hive_amount > 0 OR jesp.hbd_amount > 0
  ),
  --------------------- PREPARE FEE ---------------------
  not_approved_fees AS (
    SELECT
      ef.from_id,
      ef.escrow_id,
      ef.nai,
      ef.amount
    FROM unique_escrow_fees ef
    WHERE NOT EXISTS (
      SELECT 1
      FROM unique_escrow_approves ea
      WHERE
        ea.from_id = ef.from_id AND
        ea.escrow_id = ef.escrow_id AND
        ea.op_id > ef.op_id
    )
  ),
  filter_out_deleted_fees AS (
    SELECT
      uf.*
    FROM not_approved_fees uf
    WHERE NOT EXISTS (
      SELECT 1
      FROM union_deletions ud
      WHERE
        ud.from_id = uf.from_id AND
        ud.escrow_id = uf.escrow_id
    )
  ),
  --------------------- PREPARE FEE DELETIONS  ---------------------
  fee_approved_pre_range AS (
    SELECT
      ea.from_id,
      ea.escrow_id
    FROM unique_escrow_approves ea
    WHERE NOT EXISTS (
      SELECT 1
      FROM unique_escrow_fees ef
      WHERE
        ef.from_id = ea.from_id AND
        ef.escrow_id = ea.escrow_id AND
        ef.op_id > ea.op_id
    )
  ),
  union_fee_deletions AS (
    SELECT
      uap.from_id,
      uap.escrow_id
    FROM fee_approved_pre_range uap
    UNION ALL
    SELECT
      ud.from_id,
      ud.escrow_id
    FROM union_deletions ud

  ),
  unique_fee_deletions AS (
    SELECT DISTINCT ON (ufd.from_id, ufd.escrow_id)
      ufd.from_id,
      ufd.escrow_id
    FROM union_fee_deletions ufd
  ),
  --------------------- FINAL INSERT/UPDATE/DELETE ---------------------
  insert_escrows AS (
    INSERT INTO escrow_state (
      from_id,
      escrow_id,
      hive_nai,
      hive_amount,
      hbd_nai,
      hbd_amount,
      source_op,
      to_approved,
      disputed
    )
    SELECT
      ui.from_id,
      ui.escrow_id,
      ui.hive_nai,
      ui.hive_amount,
      ui.hbd_nai,
      ui.hbd_amount,
      ui.op_id,
      EXISTS (
        SELECT 1 FROM unique_escrow_approves uea
        WHERE uea.from_id = ui.from_id AND uea.escrow_id = ui.escrow_id
      ) AS to_approved,
      EXISTS (
        SELECT 1 FROM unique_escrow_disputes ued
        WHERE ued.from_id = ui.from_id AND ued.escrow_id = ui.escrow_id
      ) AS disputed
    FROM union_inserts ui
    ON CONFLICT ON CONSTRAINT pk_escrow_state DO UPDATE SET
      hive_amount = EXCLUDED.hive_amount,
      hbd_amount  = EXCLUDED.hbd_amount,
      source_op   = EXCLUDED.source_op,
      to_approved = escrow_state.to_approved OR EXCLUDED.to_approved,
      disputed    = escrow_state.disputed OR EXCLUDED.disputed
    RETURNING 1
  ),
  delete_escrows AS (
    DELETE FROM escrow_state es
    USING union_deletions ud
    WHERE
      es.from_id = ud.from_id AND
      es.escrow_id = ud.escrow_id
    RETURNING 1
  ),
  insert_fees AS (
    INSERT INTO escrow_fees (
      from_id,
      escrow_id,
      nai,
      fee_amount
    )
    SELECT
      ff.from_id,
      ff.escrow_id,
      ff.nai,
      ff.amount
    FROM filter_out_deleted_fees ff
    RETURNING 1
  ),
  delete_fees AS (
    DELETE FROM escrow_fees ef
    USING unique_fee_deletions ufd
    WHERE
      ef.from_id = ufd.from_id AND
      ef.escrow_id = ufd.escrow_id
    RETURNING 1
  )
  SELECT
    (SELECT COUNT(*) FROM insert_escrows),
    (SELECT COUNT(*) FROM delete_escrows),
    (SELECT COUNT(*) FROM insert_fees),
    (SELECT COUNT(*) FROM delete_fees)
  INTO __ins_escrows, __del_escrows, __ins_fee, __del_fee;

END;
$func$;

RESET ROLE;
