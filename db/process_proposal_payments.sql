SET ROLE btracker_owner;

/**
 * Records DHF proposal payments (proposal_pay_operation) in a block range.
 *
 *   - proposal_payments: one row per payment, payment_seq_no continuing from the
 *     proposal's payment_count before the range, in operation order;
 *   - proposal_payment_totals: per proposal, total_paid and payment_count added,
 *     the first payment kept, the last payment replaced.
 *
 * One statement: every CTE reads proposal_payment_totals as it was before the
 * range, which the numbering relies on. Both tables are registered, so a fork
 * rollback reverts them together.
 */
CREATE OR REPLACE FUNCTION process_block_range_proposal_payments(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _op_proposal_pay CONSTANT INT := btracker_backend.op_proposal_pay();
  __payments  INT;
  __proposals INT;
BEGIN
  WITH ops AS MATERIALIZED (
    SELECT ov.id AS source_op, ov.block_num, ov.body_value AS body
    FROM _btracker_ops_batch ov
    WHERE ov.op_type_id = _op_proposal_pay
      AND ov.block_num BETWEEN _from AND _to
  ),
  payments AS MATERIALIZED (
    SELECT
      o.source_op,
      o.block_num,
      e.proposal_id,
      e.receiver,
      e.amount,
      COALESCE(t.payment_count, 0)
        + ROW_NUMBER() OVER (PARTITION BY e.proposal_id ORDER BY o.source_op) AS payment_seq_no
    FROM ops o
    CROSS JOIN LATERAL btracker_backend.get_proposal_payment_event(o.body) e
    LEFT JOIN proposal_payment_totals t ON t.proposal_id = e.proposal_id
  ),
  insert_payments AS (
    INSERT INTO proposal_payments (proposal_id, payment_seq_no, amount, source_op)
    SELECT p.proposal_id, p.payment_seq_no, p.amount, p.source_op
    FROM payments p
    RETURNING 1
  ),
  per_proposal AS MATERIALIZED (
    SELECT
      p.proposal_id,
      p.receiver,
      SUM(p.amount)::BIGINT AS total_paid,
      COUNT(*)::INT         AS payment_count,
      MIN(p.block_num)      AS first_payment_block,
      MAX(p.block_num)      AS last_payment_block
    FROM payments p
    GROUP BY p.proposal_id, p.receiver
  ),
  upsert_totals AS (
    INSERT INTO proposal_payment_totals AS t (
      proposal_id, receiver, total_paid, payment_count,
      first_payment_block, first_payment_at, last_payment_block, last_payment_at
    )
    SELECT
      pp.proposal_id, av.id, pp.total_paid, pp.payment_count,
      pp.first_payment_block, fb.created_at, pp.last_payment_block, lb.created_at
    FROM per_proposal pp
    LEFT JOIN hive.accounts_view av ON av.name = pp.receiver
    -- The range guard lets the planner hash-join the reversible-union view instead
    -- of re-running it per row (as in process_vesting_stats).
    JOIN hive.blocks_view fb ON fb.num = pp.first_payment_block AND fb.num BETWEEN _from AND _to
    JOIN hive.blocks_view lb ON lb.num = pp.last_payment_block  AND lb.num BETWEEN _from AND _to
    ON CONFLICT ON CONSTRAINT pk_proposal_payment_totals DO UPDATE SET
      total_paid         = t.total_paid + EXCLUDED.total_paid,
      payment_count      = t.payment_count + EXCLUDED.payment_count,
      last_payment_block = EXCLUDED.last_payment_block,
      last_payment_at    = EXCLUDED.last_payment_at
    RETURNING 1
  )
  SELECT
    (SELECT count(*) FROM insert_payments),
    (SELECT count(*) FROM upsert_totals)
  INTO __payments, __proposals;
END
$$;

RESET ROLE;
