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
  _op_transfer    INT;
  _op_release     INT;
  _op_approved    INT;  
  _op_rejected    INT;  
  _op_approve_req INT;  
  _op_dispute     INT;

  __ins_new  INT := 0;
  __del_any  INT := 0;
  __upd_pre  INT := 0;

  -- single-notice metrics focused on approvals and “did agent fee matter”
  __appr_applied_keys           INT    := 0;     -- approvals that actually subtracted fee (not rejected)
  __appr_applied_sum            BIGINT := 0;     -- total approval fee subtracted (raw NAI)
  __appr_with_agent_keys        INT    := 0;     -- among those, how many had a prior agent fee
  __appr_with_agent_agent_sum   BIGINT := 0;     -- total agent fees (raw NAI) for those same escrows
  __appr_with_agent_net_delta   BIGINT := 0;     -- agent_sum - approval_sum for those escrows
BEGIN
  SELECT id INTO _op_transfer    FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_transfer_operation';
  SELECT id INTO _op_release     FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_release_operation';
  SELECT id INTO _op_approved    FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_approved_operation';
  SELECT id INTO _op_rejected    FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_rejected_operation';
  SELECT id INTO _op_approve_req FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_approve_operation';
  SELECT id INTO _op_dispute     FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_dispute_operation';

  WITH
  ops_in_range AS (
    SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, ot.name AS op_name, ov.body::jsonb AS body
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
    WHERE ov.block_num BETWEEN _from AND _to
      AND ov.op_type_id IN (_op_transfer, _op_release, _op_approved, _op_rejected, _op_approve_req, _op_dispute)
  ),

  events AS MATERIALIZED (
    SELECT e.*, o.op_id, o.block_num
    FROM ops_in_range o
    CROSS JOIN LATERAL btracker_backend.get_escrow_events(o.body, o.op_name) AS e
    WHERE e.kind IN ('transfer','release','approved','rejected')
  ),

  event_ids AS MATERIALIZED (
    SELECT
      av_from.id  AS from_id,
      av_to.id    AS to_id,
      e.escrow_id,
      e.nai,           -- may be NULL for approvals; use fee_nai for approval currency
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

  -- fee at transfer time (if any) — keep the latest transfer per key (in this batch)
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

  -- keys created (transferred) within the processed range
  create_keys_in_range AS (
    SELECT DISTINCT from_id, escrow_id, nai
    FROM event_ids
    WHERE kind = 'transfer'
  ),

  -- releases in range that apply to a state created earlier than this range
  pre_releases AS (
    SELECT r.from_id, r.escrow_id, r.nai, SUM(r.amount)::bigint AS sum_release
    FROM event_ids r
    LEFT JOIN create_keys_in_range k
      ON (k.from_id, k.escrow_id, k.nai) = (r.from_id, r.escrow_id, r.nai)
    WHERE r.kind = 'release' AND k.from_id IS NULL
    GROUP BY r.from_id, r.escrow_id, r.nai
  ),

  -- approvals in range whose creating transfer is NOT in this batch (cross-batch approvals)
  pre_approvals AS (
    SELECT a.from_id, a.escrow_id, a.nai,
           SUM(a.fee_amount)::bigint AS sum_approve_fee,
           MAX(a.op_id)              AS last_approve_op
    FROM (
      SELECT from_id,
             escrow_id,
             fee_nai AS nai,                -- approval currency = fee_nai
             op_id,
             COALESCE(fee,0)::bigint AS fee_amount
      FROM event_ids
      WHERE kind='approved' AND fee IS NOT NULL
    ) a
    LEFT JOIN create_keys_in_range k
      ON (k.from_id,k.escrow_id,k.nai)=(a.from_id,a.escrow_id,a.nai)
    WHERE k.from_id IS NULL              -- transfer NOT in this block: cross-batch approval
    GROUP BY a.from_id,a.escrow_id,a.nai
  ),

  -- compute new remaining for pre-existing rows and capture last contributing op_id (releases + approvals)
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

  -- latest transfer per key in this range (for main in-range path)
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

  -- releases after latest transfer; also compute last contributing release op_id
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

  -- if rejected after transfer -> remaining becomes 0
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

  -- approvals after the last known source_op from escrow_state (fallback when transfer not in-range)
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
     AND ev.op_id > s.source_op            -- only approvals after what we already applied
  ),

  -- unify approvals: prefer in-range transfer path, else fallback to state-based
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

  -- compute remaining and the exact last operation that determined it
  remaining_calc AS MATERIALIZED (
    SELECT
      t.from_id,
      t.escrow_id,
      t.nai,
      CASE WHEN rj.reject_op_id IS NOT NULL THEN 0
           ELSE ( t.create_amount
                + COALESCE(tf.fee_amount, 0)        -- agent fee added at transfer (unchanged)
                - ral.sum_release_after              -- releases subtract
                - COALESCE(af.fee_amount, 0) )       -- approvals subtract (unified path)
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

  -- any state that is now zero/closed (by pre-calc or main calc) or explicitly rejected (and not resurfaced as survivor)
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

  -- apply cross-batch releases/approvals to pre-existing rows, and stamp exact last contributing op_id
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
    COALESCE((SELECT COUNT(*) FROM upd_pre), 0),

    -- approvals that actually subtracted fee (i.e., not rejected after transfer) in the unified path
    COALESCE((
      SELECT COUNT(*)
      FROM remaining_calc rc
      JOIN approval_fees_after af
        ON (af.from_id, af.escrow_id, af.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      LEFT JOIN rejections_after_latest rj
        ON (rj.from_id, rj.escrow_id)=(rc.from_id, rc.escrow_id)
      WHERE COALESCE(af.fee_amount,0) > 0
        AND rj.reject_op_id IS NULL
    ), 0),
    COALESCE((
      SELECT SUM(af.fee_amount)
      FROM remaining_calc rc
      JOIN approval_fees_after af
        ON (af.from_id, af.escrow_id, af.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      LEFT JOIN rejections_after_latest rj
        ON (rj.from_id, rj.escrow_id)=(rc.from_id, rc.escrow_id)
      WHERE COALESCE(af.fee_amount,0) > 0
        AND rj.reject_op_id IS NULL
    ), 0),

    -- among those approvals, how many had a prior agent fee and its sum
    COALESCE((
      SELECT COUNT(*)
      FROM remaining_calc rc
      JOIN approval_fees_after af
        ON (af.from_id, af.escrow_id, af.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      JOIN latest_transfer_fees tf
        ON (tf.from_id, tf.escrow_id, tf.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      LEFT JOIN rejections_after_latest rj
        ON (rj.from_id, rj.escrow_id)=(rc.from_id, rc.escrow_id)
      WHERE COALESCE(af.fee_amount,0) > 0
        AND COALESCE(tf.fee_amount,0) > 0
        AND rj.reject_op_id IS NULL
    ), 0),
    COALESCE((
      SELECT SUM(tf.fee_amount)
      FROM remaining_calc rc
      JOIN approval_fees_after af
        ON (af.from_id, af.escrow_id, af.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      JOIN latest_transfer_fees tf
        ON (tf.from_id, tf.escrow_id, tf.nai)=(rc.from_id, rc.escrow_id, rc.nai)
      LEFT JOIN rejections_after_latest rj
        ON (rj.from_id, rj.escrow_id)=(rc.from_id, rc.escrow_id)
      WHERE COALESCE(af.fee_amount,0) > 0
        AND COALESCE(tf.fee_amount,0) > 0
        AND rj.reject_op_id IS NULL
    ), 0)
  INTO __ins_new, __del_any, __upd_pre,
       __appr_applied_keys, __appr_applied_sum,
       __appr_with_agent_keys, __appr_with_agent_agent_sum;

  -- compute net delta = agent_sum - approval_sum for those escrows (NULL-safe)
  __appr_with_agent_net_delta := COALESCE(__appr_with_agent_agent_sum,0) - COALESCE(__appr_applied_sum,0);

  -- Optional DEBUG (safe to remove after validation)
  RAISE NOTICE 'pre_approvals (cross-batch): rows=%, sum=%',
    (SELECT COUNT(*) FROM pre_approvals),
    (SELECT COALESCE(SUM(sum_approve_fee),0) FROM pre_approvals);

  RAISE NOTICE 'approval_fees_after (unified): rows=%, sum=%',
    (SELECT COUNT(*) FROM approval_fees_after),
    (SELECT COALESCE(SUM(fee_amount),0) FROM approval_fees_after);

  -- Single concise notice: only when there are approvals that actually subtracted fees
  IF __appr_applied_sum > 0 THEN
    RAISE NOTICE
      'escrow: approvals applied: % keys, approval_fee_subtracted=%; among these: prior agent-fee on % keys, agent_fee_sum=%; net(agent - approval)=% (raw NAI).',
      __appr_applied_keys,
      __appr_applied_sum,
      __appr_with_agent_keys,
      __appr_with_agent_agent_sum,
      __appr_with_agent_net_delta;
  END IF;

END;
$func$;

RESET ROLE;
