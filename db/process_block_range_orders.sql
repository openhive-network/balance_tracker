SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_orders(
    IN _from INT,
    IN _to INT,
    IN _report_step INT DEFAULT 1000
) 
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $func$
DECLARE
    _op_create1 INT;
    _op_create2 INT;
    _op_fill INT;
    _op_cancel INT;
    _op_cancelled INT;
    __ins_new INT := 0;
    __del_any INT := 0;
    __upd_pre INT := 0;
BEGIN
    SELECT id INTO _op_create1
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::limit_order_create_operation';

    SELECT id INTO _op_create2
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::limit_order_create2_operation';

    SELECT id INTO _op_fill
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::fill_order_operation';

    SELECT id INTO _op_cancel
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::limit_order_cancel_operation';

    SELECT id INTO _op_cancelled
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::limit_order_cancelled_operation';

    WITH
    ops_in_range AS (
        SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, (ov.body)::jsonb AS body
        FROM operations_view ov
        WHERE ov.block_num BETWEEN _from AND _to
          AND ov.op_type_id IN (_op_create1, _op_create2, _op_fill, _op_cancel, _op_cancelled)
    ),
    events AS MATERIALIZED (
        SELECT
            (SELECT av.id FROM accounts_view av WHERE av.name = e.owner) AS owner_id,
            e.owner,
            e.order_id,
            e.nai,
            e.amount,
            e.kind,
            e.side,
            o.op_id,
            o.block_num,
            o.op_type_id
        FROM ops_in_range o
        CROSS JOIN LATERAL btracker_backend.get_limit_order_events(o.body, o.op_type_id) AS e
    ),
    creates AS (
        SELECT
            owner_id,
            order_id,
            nai,
            (amount)::bigint AS create_amount,
            op_id AS create_op_id,
            block_num AS create_block
        FROM events
        WHERE kind = 'create'
    ),
    fills AS (
        SELECT
            owner_id,
            order_id,
            nai,
            (amount)::bigint AS fill_amount,
            op_id AS fill_op_id,
            block_num AS fill_block
        FROM events
        WHERE kind = 'fill'
    ),
    cancels AS (
        SELECT
            owner_id,
            order_id,
            op_id AS cancel_op_id,
            block_num AS cancel_block
        FROM events
        WHERE kind IN ('cancel', 'cancelled')
    ),
    create_keys AS (
        SELECT DISTINCT owner_id, order_id
        FROM creates
    ),
    pre_fills AS (
        SELECT
            f.owner_id,
            f.order_id,
            f.nai,
            SUM(f.fill_amount)::bigint AS sum_fill
        FROM fills f
        LEFT JOIN create_keys k USING (owner_id, order_id)
        WHERE k.owner_id IS NULL
        GROUP BY f.owner_id, f.order_id, f.nai
    ),
    pre_calc AS MATERIALIZED (
        SELECT
            s.owner_id,
            s.order_id,
            s.nai,
            s.remaining,
            pf.sum_fill,
            GREATEST(s.remaining - pf.sum_fill, 0)::bigint AS new_remaining
        FROM order_state s
        JOIN pre_fills pf
          ON (s.owner_id, s.order_id, s.nai) = (pf.owner_id, pf.order_id, pf.nai)
        WHERE pf.sum_fill <> 0
          AND s.remaining <> GREATEST(s.remaining - pf.sum_fill, 0)::bigint
    ),
    del_pre AS (
        SELECT owner_id, order_id
        FROM pre_calc
        WHERE new_remaining <= 0
    ),
    pre_canceled AS (
        SELECT DISTINCT c.owner_id, c.order_id
        FROM cancels c
        LEFT JOIN create_keys k USING (owner_id, order_id)
        WHERE k.owner_id IS NULL
    ),
    latest_creates AS MATERIALIZED (
        SELECT DISTINCT ON (owner_id, order_id, nai)
               owner_id,
               order_id,
               nai,
               create_amount,
               create_block,
               create_op_id
        FROM creates
        ORDER BY owner_id, order_id, nai, create_op_id DESC
    ),
    fills_after_latest AS (
        SELECT
            c.owner_id,
            c.order_id,
            c.nai,
            COALESCE(SUM(f.fill_amount), 0)::bigint AS sum_fill_after
        FROM latest_creates c
        LEFT JOIN fills f
          ON (f.owner_id, f.order_id, f.nai) = (c.owner_id, c.order_id, c.nai)
         AND f.fill_op_id > c.create_op_id
        GROUP BY c.owner_id, c.order_id, c.nai
    ),
    canceled_after_latest AS (
        SELECT DISTINCT c.owner_id, c.order_id
        FROM latest_creates c
        JOIN cancels k
          ON (k.owner_id, k.order_id) = (c.owner_id, c.order_id)
         AND k.cancel_op_id > c.create_op_id
    ),
    remaining_calc AS MATERIALIZED (
        SELECT
            c.owner_id,
            c.order_id,
            c.nai,
            (c.create_amount - fal.sum_fill_after)::bigint AS remaining,
            c.create_block AS block_created
        FROM latest_creates c
        JOIN fills_after_latest fal
          ON (fal.owner_id, fal.order_id, fal.nai) = (c.owner_id, c.order_id, c.nai)
    ),
    survivors AS (
        SELECT DISTINCT ON (owner_id, order_id)
               owner_id,
               order_id,
               nai,
               remaining,
               block_created
        FROM remaining_calc rc
        WHERE rc.remaining > 0
          AND NOT EXISTS (
              SELECT 1
              FROM canceled_after_latest x
              WHERE (x.owner_id, x.order_id) = (rc.owner_id, rc.order_id)
          )
        ORDER BY owner_id, order_id, block_created DESC, nai DESC
    ),
    ins_new AS (
        INSERT INTO order_state (owner_id, order_id, nai, remaining, block_created)
        SELECT owner_id, order_id, nai, remaining, block_created
        FROM survivors
        ON CONFLICT (owner_id, order_id) DO UPDATE
          SET nai = EXCLUDED.nai,
              remaining = EXCLUDED.remaining,
              block_created = EXCLUDED.block_created
          WHERE (order_state.nai,
                 order_state.remaining,
                 order_state.block_created)
                IS DISTINCT FROM (EXCLUDED.nai,
                                   EXCLUDED.remaining,
                                   EXCLUDED.block_created)
        RETURNING 1
    ),
    del_keys AS (
        SELECT owner_id, order_id FROM del_pre
        UNION ALL
        SELECT owner_id, order_id FROM pre_canceled
        UNION ALL
        SELECT owner_id, order_id FROM canceled_after_latest
        UNION ALL
        SELECT owner_id, order_id FROM remaining_calc WHERE remaining <= 0
    ),
    to_delete AS (
        SELECT DISTINCT d.owner_id, d.order_id
        FROM del_keys d
        LEFT JOIN survivors s USING (owner_id, order_id)
        WHERE s.owner_id IS NULL
    ),
    del_any AS (
        DELETE FROM order_state s
        USING to_delete d
        WHERE (s.owner_id, s.order_id) = (d.owner_id, d.order_id)
        RETURNING 1
    ),
    upd_pre AS (
        UPDATE order_state s
           SET remaining = pc.new_remaining
          FROM pre_calc pc
         WHERE (s.owner_id, s.order_id, s.nai) = (pc.owner_id, pc.order_id, pc.nai)
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
