SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.prev_balance(
    _prev_balance BIGINT,
    _account_id INT,
    _coin_type INT,
    _from_seq INT
)
RETURNS BIGINT
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  IF _prev_balance IS NOT NULL THEN
    RETURN _prev_balance;
  END IF;

  _prev_balance := (
    SELECT abh.balance
    FROM account_balance_history abh
    WHERE
      abh.account = _account_id AND
      abh.nai = _coin_type AND
      abh.balance_seq_no < _from_seq
    ORDER BY abh.balance_seq_no DESC
    LIMIT 1
  );

  RETURN COALESCE(_prev_balance, 0);
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.liquid_balance_history(
    _account_id INT,
    _coin_type INT,
    _page INT,
    _page_size INT,
    _order_is btracker_backend.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS btracker_backend.operation_history
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _result          btracker_backend.balance_history[];
  _bh_range        btracker_backend.balance_history_range_return;
  _calculate_pages btracker_backend.calculate_pages_return;
BEGIN
  -----------PAGING LOGIC----------------
  _bh_range        := btracker_backend.balance_history_range(_account_id, _coin_type, 'balance', _from_block, _to_block);
  _calculate_pages := btracker_backend.calculate_pages(_bh_range.count, _page, _order_is, _page_size);

  -- Fetching operations
	WITH gather_page AS MATERIALIZED (
    SELECT
      ab.balance_seq_no,
      ab.balance,
      ab.source_op,
      ab.source_op_block,
      hafd.operation_id_to_type_id(ab.source_op) AS op_type_id
    FROM account_balance_history ab
    WHERE ab.account         = _account_id
      AND ab.nai             = _coin_type
      AND ab.balance_seq_no >= _bh_range.from_seq
      AND ab.balance_seq_no <= _bh_range.to_seq
      AND (_order_is = 'desc' OR ab.balance_seq_no >= _bh_range.from_seq + _calculate_pages.offset_filter)
      AND (_order_is = 'asc' OR ab.balance_seq_no <= _bh_range.to_seq - _calculate_pages.offset_filter)
    ORDER BY
      (CASE WHEN _order_is = 'desc' THEN ab.balance_seq_no ELSE NULL END) DESC,
      (CASE WHEN _order_is = 'asc'  THEN ab.balance_seq_no ELSE NULL END) ASC
    LIMIT _calculate_pages.limit_filter + 1 -- extra row for prev balance calculation
  ),
  --------------------------
  --calculate prev balance--
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
      (CASE WHEN _order_is = 'desc' THEN current.balance_seq_no ELSE NULL END) DESC,
      (CASE WHEN _order_is = 'asc'  THEN current.balance_seq_no ELSE NULL END) ASC
    LIMIT _calculate_pages.limit_filter
  ),
  check_if_prev_balance_is_null AS (
    SELECT
      jpb.source_op_block,
      jpb.source_op,
      jpb.op_type_id,
      jpb.balance,
      btracker_backend.prev_balance(jpb.prev_balance, _account_id, _coin_type, jpb.balance_seq_no) AS prev_balance,
      bv.created_at
    FROM join_prev_balance jpb
    JOIN hive.blocks_view bv ON bv.num = jpb.source_op_block
  )
  SELECT array_agg(rows ORDER BY
    (CASE WHEN _order_is = 'desc' THEN rows.source_op::BIGINT ELSE NULL END) DESC,
    (CASE WHEN _order_is = 'asc'  THEN rows.source_op::BIGINT ELSE NULL END) ASC
  )
  INTO _result
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
  ) rows;

  ----------------------------------------
  RETURN (
    COALESCE(_bh_range.count, 0),
    COALESCE(_calculate_pages.total_pages, 0),
    COALESCE(_result, '{}'::btracker_backend.balance_history[])
  )::btracker_backend.operation_history;

END
$$;

RESET ROLE;
