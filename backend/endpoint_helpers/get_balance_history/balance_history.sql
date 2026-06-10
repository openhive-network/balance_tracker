SET ROLE btracker_owner;

-- Paginated balance history reader (liquid + savings unified) for get_balance_history.

/*
Paginated balance history reader — liquid and savings unified.
Called by: btracker_endpoints.get_balance_history().

Liquid and savings balance history differ ONLY in which history view/table they read, so
they share one implementation here. The gather_page view is selected by _balance_type
(whitelisted — never raw request input); everything else is written once:
  - windowed seq_no scan (BETWEEN bounds, no OFFSET keyword)
  - LIMIT+1 extra row so LAG() can compute prev_balance for the first result row
  - prev_balance() fallback for the very first row (no previous row to LAG from)
  - balance_change = balance - prev_balance projection

Perf-critical invariants preserved from the original liquid/savings split:
  - the +1-row LAG() trick and calculate_pages() direction math are byte-identical
  - the prev_balance fallback hits the BASE table (not the view) for an index-only scan
    on (account, nai, balance_seq_no)
*/

-- Paginated reader. _balance_type picks the history view; the body is written once.
CREATE OR REPLACE FUNCTION btracker_backend.balance_history(
    _account_id   INT,
    _coin_type    INT,
    _balance_type btracker_backend.balance_type,
    _page         INT,
    _page_size    INT,
    _order_is     btracker_backend.sort_direction,
    _from_block   INT,
    _to_block     INT
)
RETURNS btracker_backend.operation_history
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _result          btracker_backend.balance_history[];
  _bh_range        btracker_backend.balance_history_range_return;
  _calculate_pages btracker_backend.calculate_pages_return;
  _view            TEXT := CASE _balance_type
                             WHEN 'balance' THEN 'account_balance_history_view'
                             ELSE 'account_savings_history_view'
                           END;
BEGIN
  -- Step 1: block range -> seq_no bounds + total count (dispatches on _balance_type internally).
  _bh_range        := btracker_backend.balance_history_range(_account_id, _coin_type, _balance_type, _from_block, _to_block);
  -- Step 2: direction-aware offset/limit in seq_no space.
  _calculate_pages := btracker_backend.calculate_pages(_bh_range.count, _page, _order_is, _page_size);

  -- Step 3-6: windowed page scan (+1 row) -> LAG() prev_balance -> fallback -> balance_change.
  -- Only the history view differs between liquid and savings, so it is the sole injected token.
  EXECUTE format($q$
    WITH gather_page AS MATERIALIZED (
      SELECT
        ab.balance_seq_no,
        ab.balance,
        ab.source_op,
        ab.source_op_block,
        ops.op_type_id
      FROM btracker_backend.%1$I ab
      JOIN hafd.operations ops ON ops.id = ab.source_op
      WHERE ab.account         = $1
        AND ab.nai             = $2
        AND ab.balance_seq_no >= $3
        AND ab.balance_seq_no <= $4
        AND ($5 = 'desc' OR ab.balance_seq_no >= $3 + $6)
        AND ($5 = 'asc'  OR ab.balance_seq_no <= $4 - $6)
      ORDER BY
        (CASE WHEN $5 = 'desc' THEN ab.balance_seq_no ELSE NULL END) DESC,
        (CASE WHEN $5 = 'asc'  THEN ab.balance_seq_no ELSE NULL END) ASC
      LIMIT $7 + 1
    ),
    join_prev_balance AS (
      SELECT
        current.balance_seq_no,
        current.source_op_block,
        current.source_op,
        current.op_type_id,
        current.balance,
        LAG(current.balance) OVER (ORDER BY current.balance_seq_no) AS prev_balance
      FROM gather_page current
      ORDER BY
        (CASE WHEN $5 = 'desc' THEN current.balance_seq_no ELSE NULL END) DESC,
        (CASE WHEN $5 = 'asc'  THEN current.balance_seq_no ELSE NULL END) ASC
      LIMIT $7
    ),
    check_if_prev_balance_is_null AS (
      SELECT
        jpb.source_op_block,
        jpb.source_op,
        jpb.op_type_id,
        jpb.balance,
        btracker_backend.prev_balance($8, jpb.prev_balance, $1, $2, jpb.balance_seq_no) AS prev_balance,
        bv.created_at
      FROM join_prev_balance jpb
      JOIN hive.blocks_view bv ON bv.num = jpb.source_op_block
    )
    SELECT array_agg(rows ORDER BY
      (CASE WHEN $5 = 'desc' THEN rows.source_op::BIGINT ELSE NULL END) DESC,
      (CASE WHEN $5 = 'asc'  THEN rows.source_op::BIGINT ELSE NULL END) ASC
    )
    FROM (
      SELECT
        s.source_op_block,
        s.source_op::TEXT,
        s.op_type_id,
        s.balance,
        s.prev_balance,
        (s.balance - s.prev_balance) AS balance_change,
        s.created_at
      FROM check_if_prev_balance_is_null s
    ) rows
  $q$, _view)
  INTO _result
  USING _account_id, _coin_type, _bh_range.from_seq, _bh_range.to_seq, _order_is,
        _calculate_pages.offset_filter, _calculate_pages.limit_filter, _balance_type;

  RETURN (
    COALESCE(_bh_range.count, 0),
    COALESCE(_calculate_pages.total_pages, 0),
    COALESCE(_result, '{}'::btracker_backend.balance_history[])
  )::btracker_backend.operation_history;
END
$$;

RESET ROLE;
