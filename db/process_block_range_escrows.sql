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
  _op_transfer INT;
  _op_release  INT;
  _op_approved INT;
  _op_rejected INT;

  __ins_new  INT := 0;
  __del_any  INT := 0;
  __upd_pre  INT := 0;
BEGIN
  SELECT id INTO _op_transfer FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_transfer_operation';
  SELECT id INTO _op_release  FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_release_operation';
  SELECT id INTO _op_approved FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_approved_operation';
  SELECT id INTO _op_rejected FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_rejected_operation';

  WITH
  ops_in_range AS (
    SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, ot.name AS op_name, ov.body::jsonb AS body
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
    WHERE ov.block_num BETWEEN _from AND _to
      AND ov.op_type_id IN (_op_transfer, _op_release, _op_approved, _op_rejected)
  ),

  events AS MATERIALIZED (
    SELECT e.*, o.op_id, o.block_num
    FROM ops_in_range o
    CROSS JOIN LATERAL (
      SELECT (btracker_backend.get_escrow_events(o.body, o.op_name)).*
    ) AS e
    WHERE e.kind IN ('transfer','release','approved','rejected')
  ),


  event_ids AS MATERIALIZED (
    SELECT
      av_from.id  AS from_id,
      av_to.id    AS to_id,
      e.escrow_id,
      e.nai,
      e.amount,
      e.kind,
      e.fee,
      e.fee_nai,
      e.op_id,
      e.block_num
    FROM events e
    JOIN hive.accounts_view av_from ON av_from.name = e.from_name
    LEFT JOIN hive.accounts_view av_to   ON av_to.name   = e.to_name
  ),

  /* ---------- FEES AT TRANSFER (agent) ---------- */
  transfer_rows AS (
    SELECT from_id, escrow_id, fee_nai AS nai,
           COALESCE(fee,0)::bigint AS fee_amount,
           op_id, block_num
    FROM event_ids
    WHERE kind = 'transfer' AND fee IS NOT NULL
  ),
  latest_transfer_fees AS MATERIALIZED (
    SELECT DISTINCT ON (from_id, escrow_id, nai)
           from_id, escrow_id, nai, fee_amount, op_id AS create_op_id
    FROM transfer_rows
    ORDER BY from_id, escrow_id, nai, op_id DESC
  ),

  /* ---------- KEYS CREATED IN-RANGE ---------- */
  create_keys_in_range AS (
    SELECT DISTINCT from_id, escrow_id, nai
    FROM event_ids
    WHERE kind = 'transfer'
  ),

  /* ---------- PRE-RANGE RELEASES ---------- */
  pre_releases AS (
    SELECT r.from_id, r.escrow_id, r.nai, SUM(r.amount)::bigint AS sum_release
    FROM event_ids r
    LEFT JOIN create_keys_in_range k
      ON (k.from_id, k.escrow_id, k.nai) = (r.from_id, r.escrow_id, r.nai)
    WHERE r.kind = 'release' AND k.from_id IS NULL
    GROUP BY r.from_id, r.escrow_id, r.nai
  ),

  /* ---------- PRE-RANGE APPROVAL FEES (RICHER BEHAVIOR) ---------- */
  pre_approvals AS (
    SELECT a.from_id, a.escrow_id, a.nai,
           SUM(a.fee_amount)::bigint AS sum_approve_fee,
           MAX(a.op_id)              AS last_approve_op
    FROM (
      SELECT from_id,
             escrow_id,
             fee_nai AS nai,
             op_id,
             COALESCE(fee,0)::bigint AS fee_amount
      FROM event_ids
      WHERE kind='approved' AND fee IS NOT NULL
    ) a
    LEFT JOIN create_keys_in_range k
      ON (k.from_id,k.escrow_id,k.nai)=(a.from_id,a.escrow_id,a.nai)
    WHERE k.from_id IS NULL
    GROUP BY a.from_id,a.escrow_id,a.nai
  ),

  /* ---------- APPLY PRE-RANGE RELEASES + APPROVAL FEES TO EXISTING STATE ---------- */
  pre_calc AS MATERIALIZED (
    SELECT s.from_id,
           s.escrow_id,
           s.nai,
           s.remaining,
           COALESCE(pr.sum_release,0)     AS sum_release,
           COALESCE(pa.sum_approve_fee,0) AS sum_approve_fee,
           GREATEST(
             s.remaining - COALESCE(pr.sum_release,0) - COALESCE(pa.sum_approve_fee,0),
             0
           )::bigint AS new_remaining,
           GREATEST(
             COALESCE((
               SELECT MAX(ev.op_id)
               FROM event_ids ev
               WHERE ev.kind='release'
                 AND (ev.from_id,ev.escrow_id,ev.nai)=(s.from_id,s.escrow_id,s.nai)
             ), 0),
             COALESCE(pa.last_approve_op, 0)
           ) AS pre_last_op_id
    FROM escrow_state s
    LEFT JOIN pre_releases  pr ON (s.from_id,s.escrow_id,s.nai)=(pr.from_id,pr.escrow_id,pr.nai)
    LEFT JOIN pre_approvals pa ON (s.from_id,s.escrow_id,s.nai)=(pa.from_id,pa.escrow_id,pa.nai)
    WHERE COALESCE(pr.sum_release,0) <> 0
       OR COALESCE(pa.sum_approve_fee,0) <> 0
  ),

  /* ---------- IN-RANGE TRANSFERS & AFTER-EVENTS ---------- */
  latest_transfers AS MATERIALIZED (
    SELECT DISTINCT ON (from_id, escrow_id, nai)
           from_id, escrow_id, nai,
           amount::bigint AS create_amount,
           op_id          AS create_op_id,
           block_num      AS create_block
    FROM event_ids
    WHERE kind = 'transfer'
    ORDER BY from_id, escrow_id, nai, op_id DESC
  ),

  releases_after_latest AS MATERIALIZED (
    WITH rel AS (
      SELECT r.from_id, r.escrow_id, r.nai, r.amount, r.op_id, r.block_num
      FROM event_ids r
      JOIN latest_transfers t
        ON (r.from_id, r.escrow_id, r.nai) = (t.from_id, t.escrow_id, t.nai)
       AND r.kind = 'release'
       AND r.op_id > t.create_op_id
    )
    SELECT
      t.from_id, t.escrow_id, t.nai,
      COALESCE(SUM(rel.amount),0)::bigint AS sum_release_after,
      (SELECT rel.op_id
         FROM rel
        WHERE (rel.from_id,rel.escrow_id,rel.nai)=(t.from_id,t.escrow_id,t.nai)
        ORDER BY rel.op_id DESC
        LIMIT 1) AS last_release_op
    FROM latest_transfers t
    LEFT JOIN rel
      ON (rel.from_id, rel.escrow_id, rel.nai) = (t.from_id, t.escrow_id, t.nai)
    GROUP BY t.from_id, t.escrow_id, t.nai
  ),

  rejections_after_latest AS MATERIALIZED (
    SELECT t.from_id, t.escrow_id,
           MAX(ev.op_id) AS reject_op_id
    FROM latest_transfers t
    JOIN event_ids ev
      ON ev.from_id   = t.from_id
     AND ev.escrow_id = t.escrow_id
     AND ev.op_id     > t.create_op_id
     AND ev.kind      = 'rejected'
    GROUP BY t.from_id, t.escrow_id
  ),

  /* ---------- APPROVAL FEES AFTER (RICHER BEHAVIOR) ---------- */
  approval_fees_after_latest AS MATERIALIZED (
    SELECT DISTINCT ON (t.from_id, t.escrow_id, t.nai)
           t.from_id, t.escrow_id, t.nai,
           COALESCE(ev.fee,0)::bigint AS fee_amount,
           ev.op_id                   AS approve_op_id
    FROM latest_transfers t
    JOIN event_ids ev
      ON ev.from_id   = t.from_id
     AND ev.escrow_id = t.escrow_id
     AND ev.op_id     > t.create_op_id
     AND ev.kind      = 'approved'
     AND ev.fee IS NOT NULL
     AND ev.fee_nai   = t.nai
    ORDER BY t.from_id, t.escrow_id, t.nai, ev.op_id DESC
  ),

  approval_fees_after_state AS MATERIALIZED (
    SELECT s.from_id, s.escrow_id, s.nai,
           COALESCE(ev.fee,0)::bigint AS fee_amount,
           ev.op_id                   AS approve_op_id
    FROM escrow_state s
    JOIN event_ids ev
      ON ev.kind = 'approved'
     AND ev.fee IS NOT NULL
     AND ev.fee_nai = s.nai
     AND ev.from_id = s.from_id
     AND ev.escrow_id = s.escrow_id
     AND ev.op_id > s.source_op
  ),

  approval_fees_after AS MATERIALIZED (
    SELECT * FROM approval_fees_after_latest
    UNION ALL
    SELECT afs.*
    FROM approval_fees_after_state afs
    WHERE NOT EXISTS (
      SELECT 1 FROM latest_transfers lt
      WHERE (lt.from_id,lt.escrow_id,lt.nai)=(afs.from_id,afs.escrow_id,afs.nai)
    )
  ),

  /* ---------- REMAINING + LAST OP (USES RICHER APPROVAL COVERAGE) ---------- */
  remaining_calc AS MATERIALIZED (
    SELECT
      t.from_id,
      t.escrow_id,
      t.nai,
      CASE WHEN rj.reject_op_id IS NOT NULL THEN 0
           ELSE ( t.create_amount
                + COALESCE(tf.fee_amount, 0)          -- transfer-time fee
                - ral.sum_release_after                -- releases after
                - COALESCE(af.fee_amount, 0) )         -- approval fee after (latest OR state)
      END AS remaining,
      GREATEST(
        t.create_op_id,
        COALESCE(ral.last_release_op, t.create_op_id),
        COALESCE(af.approve_op_id, t.create_op_id),
        COALESCE(rj.reject_op_id, 0)
      ) AS last_op_id
    FROM latest_transfers t
    JOIN releases_after_latest ral
      ON (ral.from_id, ral.escrow_id, ral.nai) = (t.from_id, t.escrow_id, t.nai)
    LEFT JOIN latest_transfer_fees tf
      ON (tf.from_id, tf.escrow_id, tf.nai) = (t.from_id, t.escrow_id, t.nai)
    LEFT JOIN approval_fees_after af
      ON (af.from_id, af.escrow_id, af.nai) = (t.from_id, t.escrow_id, t.nai)
    LEFT JOIN rejections_after_latest rj
      ON (rj.from_id, rj.escrow_id) = (t.from_id, t.escrow_id)
  ),

  transfer_to_ids AS (
    SELECT DISTINCT ON (from_id, escrow_id, nai)
           from_id, escrow_id, nai, to_id
    FROM event_ids
    WHERE kind = 'transfer'
    ORDER BY from_id, escrow_id, nai, op_id DESC
  ),

  survivors AS (
    SELECT rc.from_id, rc.escrow_id, rc.nai,
           tt.to_id,
           rc.remaining,
           rc.last_op_id AS source_op
    FROM remaining_calc rc
    JOIN transfer_to_ids tt
      ON (tt.from_id, tt.escrow_id, tt.nai) = (rc.from_id, rc.escrow_id, rc.nai)
    WHERE rc.remaining > 0
  ),

  ins_new AS (
    INSERT INTO escrow_state (from_id, escrow_id, nai, to_id, remaining, source_op)
    SELECT from_id, escrow_id, nai, to_id, remaining, source_op
    FROM survivors
    ON CONFLICT (from_id, escrow_id, nai) DO UPDATE
      SET to_id     = EXCLUDED.to_id,
          remaining = EXCLUDED.remaining,
          source_op = EXCLUDED.source_op
    RETURNING 1
  ),

  pre_rejected AS (
    SELECT s.from_id, s.escrow_id
    FROM escrow_state s
    WHERE EXISTS (
      SELECT 1
      FROM event_ids ev
      WHERE ev.kind = 'rejected'
        AND ev.escrow_id = s.escrow_id
        AND ev.from_id   = s.from_id
    )
  ),

  del_keys AS (
    SELECT from_id, escrow_id FROM (
      SELECT from_id, escrow_id FROM pre_calc       WHERE new_remaining <= 0
      UNION ALL
      SELECT from_id, escrow_id FROM remaining_calc WHERE remaining     <= 0
      UNION ALL
      SELECT from_id, escrow_id FROM pre_rejected
    ) u
  ),

  to_delete AS (
    SELECT DISTINCT d.from_id, d.escrow_id
    FROM del_keys d
    LEFT JOIN survivors s USING (from_id, escrow_id)
    WHERE s.from_id IS NULL
  ),

  del_any AS (
    DELETE FROM escrow_state s
    USING to_delete d
    WHERE (s.from_id, s.escrow_id) = (d.from_id, d.escrow_id)
    RETURNING 1
  ),

  upd_pre AS (
    UPDATE escrow_state s
       SET remaining = pc.new_remaining,
           source_op = COALESCE(pc.pre_last_op_id, s.source_op)
      FROM pre_calc pc
     WHERE (s.from_id, s.escrow_id, s.nai) = (pc.from_id, pc.escrow_id, pc.nai)
       AND pc.new_remaining > 0
       AND s.remaining <> pc.new_remaining
    RETURNING 1
  )

  SELECT
    COALESCE((SELECT COUNT(*) FROM ins_new), 0),
    COALESCE((SELECT COUNT(*) FROM del_any), 0),
    COALESCE((SELECT COUNT(*) FROM upd_pre), 0)
  INTO __ins_new, __del_any, __upd_pre;

END;
$func$;

RESET ROLE;
