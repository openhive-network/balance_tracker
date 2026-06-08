SET ROLE btracker_owner;

/*
Per-account vesting event history.
Called by: btracker_endpoints.get_account_vesting_history()

Reads from account_vesting_history (populated during sync) using
WINDOWED-by-seq_no scan (no OFFSET keyword), mirroring liquid_balance_history's
pagination strategy:

  1. vesting_history_range(_account, _kind, _from_block, _to_block) converts
     the optional block-range filter into seq_no bounds (count, from_seq,
     to_seq) — O(log n) via composite indexes.
  2. calculate_pages(count, page, direction, page_size) gives offset/limit
     in seq_no space.
  3. Page query: WHERE account=? [AND kind=?] AND seq_no BETWEEN
     (from_seq + offset) AND (to_seq - offset)  ordered, LIMIT only.
     No OFFSET keyword — index range scan, O(page_size) regardless of depth.

Two seq_no columns drive this:
  * vesting_seq_no — per (account), monotonic across kinds, used for filter='all'
  * kind_seq_no    — per (account, kind), monotonic, used for filter=specific

Both populated at sync time by process_account_vesting_stats.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_account_vesting_history(
    _account_id  INT,
    _filter      btracker_backend.vesting_filter,
    _page        INT,
    _page_size   INT,
    _direction   btracker_backend.sort_direction,
    _from_block  INT,
    _to_block    INT
)
RETURNS btracker_backend.vesting_history
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _kind_filter    SMALLINT := CASE _filter
                              WHEN 'all'                       THEN NULL
                              WHEN 'power_up'                  THEN 1::SMALLINT
                              WHEN 'power_down_init'           THEN 2::SMALLINT
                              WHEN 'power_down_fill'           THEN 3::SMALLINT
                              WHEN 'power_down_route_received' THEN 4::SMALLINT
                            END;
  _nai_hive       INT := btracker_backend.nai_hive();
  _nai_vests      INT := btracker_backend.nai_vests();
  _vh_range       btracker_backend.balance_history_range_return;
  _calc           btracker_backend.calculate_pages_return;
  _result         btracker_backend.vesting_history_event[];
BEGIN
  -- Step 1: block-range -> seq_no range (count + from_seq + to_seq).
  -- For filter='all' the seq is vesting_seq_no; for filter=specific kind it
  -- is kind_seq_no for that (account, kind). Function handles both internally.
  _vh_range := btracker_backend.vesting_history_range(_account_id, _kind_filter, _from_block, _to_block);

  -- Step 2: offset/limit in seq_no space (direction-aware via calculate_pages).
  _calc := btracker_backend.calculate_pages(_vh_range.count, _page, _direction, _page_size);

  -- Step 3: windowed page slice — split on _kind_filter so each branch uses
  -- its dedicated composite index and avoids OR-based plan degradation.
  IF _kind_filter IS NULL THEN
    -- filter='all': scan via (account, vesting_seq_no) UNIQUE index.
    WITH gather_page AS MATERIALIZED (
      SELECT
        avh.vesting_seq_no AS page_seq,
        avh.kind,
        avh.hive_amount,
        avh.vests_amount,
        avh.source_op,
        hafd.operation_id_to_block_num(avh.source_op) AS block_num
      FROM account_vesting_history avh
      WHERE avh.account = _account_id
        AND avh.vesting_seq_no >= _vh_range.from_seq
        AND avh.vesting_seq_no <= _vh_range.to_seq
        AND (_direction = 'desc' OR avh.vesting_seq_no >= _vh_range.from_seq + _calc.offset_filter)
        AND (_direction = 'asc'  OR avh.vesting_seq_no <= _vh_range.to_seq   - _calc.offset_filter)
      ORDER BY
        (CASE WHEN _direction = 'desc' THEN avh.vesting_seq_no ELSE NULL END) DESC,
        (CASE WHEN _direction = 'asc'  THEN avh.vesting_seq_no ELSE NULL END) ASC
      LIMIT _calc.limit_filter
    )
    SELECT array_agg(row(
      gp.block_num,
      gp.source_op::TEXT,
      (CASE gp.kind
        WHEN 1 THEN btracker_backend.op_transfer_to_vesting()
        WHEN 2 THEN btracker_backend.op_withdraw_vesting()
        WHEN 3 THEN btracker_backend.op_fill_vesting_withdraw()
        WHEN 4 THEN btracker_backend.op_fill_vesting_withdraw()
      END)::SMALLINT,
      (CASE gp.kind
        WHEN 1 THEN 'power_up'
        WHEN 2 THEN 'power_down_init'
        WHEN 3 THEN 'power_down_fill'
        WHEN 4 THEN 'power_down_route_received'
      END)::btracker_backend.vesting_filter,
      btracker_backend.create_amount_object(_nai_hive,  gp.hive_amount),
      btracker_backend.create_amount_object(_nai_vests, gp.vests_amount),
      bv.created_at::TIMESTAMP
    )::btracker_backend.vesting_history_event ORDER BY
      (CASE WHEN _direction = 'desc' THEN gp.page_seq ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN gp.page_seq ELSE NULL END) ASC
    )
    INTO _result
    FROM gather_page gp
    JOIN hive.blocks_view bv ON bv.num = gp.block_num;

  ELSE
    -- filter=specific: scan via (account, kind, kind_seq_no) UNIQUE index.
    WITH gather_page AS MATERIALIZED (
      SELECT
        avh.kind_seq_no AS page_seq,
        avh.kind,
        avh.hive_amount,
        avh.vests_amount,
        avh.source_op,
        hafd.operation_id_to_block_num(avh.source_op) AS block_num
      FROM account_vesting_history avh
      WHERE avh.account = _account_id
        AND avh.kind    = _kind_filter
        AND avh.kind_seq_no >= _vh_range.from_seq
        AND avh.kind_seq_no <= _vh_range.to_seq
        AND (_direction = 'desc' OR avh.kind_seq_no >= _vh_range.from_seq + _calc.offset_filter)
        AND (_direction = 'asc'  OR avh.kind_seq_no <= _vh_range.to_seq   - _calc.offset_filter)
      ORDER BY
        (CASE WHEN _direction = 'desc' THEN avh.kind_seq_no ELSE NULL END) DESC,
        (CASE WHEN _direction = 'asc'  THEN avh.kind_seq_no ELSE NULL END) ASC
      LIMIT _calc.limit_filter
    )
    SELECT array_agg(row(
      gp.block_num,
      gp.source_op::TEXT,
      (CASE gp.kind
        WHEN 1 THEN btracker_backend.op_transfer_to_vesting()
        WHEN 2 THEN btracker_backend.op_withdraw_vesting()
        WHEN 3 THEN btracker_backend.op_fill_vesting_withdraw()
        WHEN 4 THEN btracker_backend.op_fill_vesting_withdraw()
      END)::SMALLINT,
      (CASE gp.kind
        WHEN 1 THEN 'power_up'
        WHEN 2 THEN 'power_down_init'
        WHEN 3 THEN 'power_down_fill'
        WHEN 4 THEN 'power_down_route_received'
      END)::btracker_backend.vesting_filter,
      btracker_backend.create_amount_object(_nai_hive,  gp.hive_amount),
      btracker_backend.create_amount_object(_nai_vests, gp.vests_amount),
      bv.created_at::TIMESTAMP
    )::btracker_backend.vesting_history_event ORDER BY
      (CASE WHEN _direction = 'desc' THEN gp.page_seq ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN gp.page_seq ELSE NULL END) ASC
    )
    INTO _result
    FROM gather_page gp
    JOIN hive.blocks_view bv ON bv.num = gp.block_num;
  END IF;

  RETURN (
    COALESCE(_vh_range.count, 0),
    COALESCE(_calc.total_pages, 0),
    COALESCE(_result, '{}'::btracker_backend.vesting_history_event[])
  )::btracker_backend.vesting_history;
END
$$;

RESET ROLE;
