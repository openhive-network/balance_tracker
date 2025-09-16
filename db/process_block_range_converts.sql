SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_converts(
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
AS $func$
DECLARE
    _op_convert  INT;
    _op_fillconv INT;
    __upd_pre    INT := 0;
    __ins_new    INT := 0;
    __del_any    INT := 0;
BEGIN
    SELECT id INTO _op_convert
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::convert_operation';

    SELECT id INTO _op_fillconv
    FROM hafd.operation_types
    WHERE name = 'hive::protocol::fill_convert_request_operation';

    WITH
    ops_in_range AS (
        SELECT ov.id AS op_id, ov.block_num, ov.op_type_id, ov.body
        FROM hive.operations_view ov
        WHERE ov.block_num BETWEEN _from AND _to
          AND ov.op_type_id IN (_op_convert, _op_fillconv)
    ),
    events AS MATERIALIZED (
        SELECT
            av.id AS owner_id,
            e.owner,
            e.request_id,
            e.nai,
            e.amount_in,
            o.op_type_id,
            o.op_id,
            o.block_num,
            o.body
        FROM ops_in_range o
        CROSS JOIN LATERAL btracker_backend.get_convert_events(o.body, o.op_type_id) AS e
        JOIN accounts_view av ON av.name = e.owner
    ),
    creates AS (
        SELECT
            owner_id,
            request_id,
            nai,
            e.amount_in AS create_amount,         
            op_id AS create_op_id,
            block_num AS create_block
        FROM events e
        WHERE op_type_id = _op_convert
    ),
    fills AS (
        SELECT
            owner_id,
            request_id,
            nai,
            e.amount_in AS fill_amount,           
            op_id AS fill_op_id,
            block_num AS fill_block
        FROM events e
        WHERE op_type_id = _op_fillconv
    ),
    create_keys AS (
        SELECT DISTINCT owner_id, request_id, nai
        FROM creates
    ),
    pre_fills AS (
        SELECT
            f.owner_id,
            f.request_id,
            f.nai,
            SUM(f.fill_amount)::bigint AS sum_fill
        FROM fills f
        LEFT JOIN create_keys k USING (owner_id, request_id, nai)
        WHERE k.owner_id IS NULL
        GROUP BY f.owner_id, f.request_id, f.nai
    ),
    pre_calc AS MATERIALIZED (
        SELECT
            s.owner_id,
            s.request_id,
            s.nai,
            s.remaining,
            pf.sum_fill,
            GREATEST(s.remaining - pf.sum_fill, 0)::bigint AS new_remaining
        FROM convert_state s
        JOIN pre_fills pf
          ON (s.owner_id, s.request_id, s.nai) = (pf.owner_id, pf.request_id, pf.nai)
        WHERE pf.sum_fill <> 0
          AND s.remaining <> GREATEST(s.remaining - pf.sum_fill, 0)::bigint
    ),
    upd_pre AS (
        UPDATE convert_state s
           SET remaining = pc.new_remaining
          FROM pre_calc pc
         WHERE (s.owner_id, s.request_id, s.nai) = (pc.owner_id, pc.request_id, pc.nai)
           AND s.remaining <> pc.new_remaining
           AND pc.new_remaining > 0
        RETURNING 1
    ),
    pre_zero AS (
        SELECT owner_id, request_id, nai
        FROM pre_calc
        WHERE new_remaining <= 0
    ),
    latest_creates AS MATERIALIZED (
        SELECT DISTINCT ON (owner_id, request_id, nai)
               owner_id,
               request_id,
               nai,
               create_amount,
               create_block,
               create_op_id
        FROM creates
        ORDER BY owner_id, request_id, nai, create_op_id DESC
    ),
    fills_after_latest AS (
        SELECT
            c.owner_id,
            c.request_id,
            c.nai,
            COALESCE(SUM(f.fill_amount), 0)::bigint AS sum_fill_after
        FROM latest_creates c
        LEFT JOIN fills f
          ON (f.owner_id, f.request_id, f.nai) = (c.owner_id, c.request_id, c.nai)
         AND f.fill_op_id > c.create_op_id
        GROUP BY c.owner_id, c.request_id, c.nai
    ),
    remaining_calc AS MATERIALIZED (
        SELECT
            c.owner_id,
            c.request_id,
            c.nai,
            (c.create_amount - fal.sum_fill_after)::bigint AS remaining,
            c.create_block AS request_block
        FROM latest_creates c
        JOIN fills_after_latest fal
          ON (fal.owner_id, fal.request_id, fal.nai) = (c.owner_id, c.request_id, c.nai)
    ),
    survivors AS (
        SELECT owner_id, request_id, nai, remaining, request_block
        FROM remaining_calc
        WHERE remaining > 0
    ),
    ins_new AS (
        INSERT INTO convert_state (owner_id, request_id, nai, remaining, request_block)
        SELECT owner_id, request_id, nai, remaining, request_block
        FROM survivors
        ON CONFLICT (owner_id, request_id, nai) DO UPDATE
          SET remaining     = EXCLUDED.remaining,
              request_block = EXCLUDED.request_block
          WHERE (convert_state.remaining, convert_state.request_block)
                IS DISTINCT FROM (EXCLUDED.remaining, EXCLUDED.request_block)
        RETURNING 1
    ),
    keys_to_delete_raw AS (
        SELECT owner_id, request_id, nai FROM pre_zero
    ),
    to_delete AS (
        SELECT DISTINCT d.owner_id, d.request_id, d.nai
        FROM keys_to_delete_raw d
        LEFT JOIN survivors s USING (owner_id, request_id, nai)
        WHERE s.owner_id IS NULL
    ),
    del_any AS (
        DELETE FROM convert_state s
        USING to_delete d
        WHERE (s.owner_id, s.request_id, s.nai) = (d.owner_id, d.request_id, d.nai)
        RETURNING 1
    )
    SELECT
        COALESCE((SELECT COUNT(*) FROM upd_pre), 0),
        COALESCE((SELECT COUNT(*) FROM ins_new), 0),
        COALESCE((SELECT COUNT(*) FROM del_any), 0)
    INTO __upd_pre, __ins_new, __del_any;
END;
$func$;

RESET ROLE;
