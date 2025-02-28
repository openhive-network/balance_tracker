-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

DROP TYPE IF EXISTS balance_history_aggregation CASCADE;
CREATE TYPE balance_history_aggregation AS (
    date TIMESTAMP,
    balance BIGINT,
    prev_balance BIGINT,
    min_balance BIGINT,
    max_balance BIGINT
);

CREATE OR REPLACE VIEW balance_history_by_year AS
WITH get_year AS (
    SELECT
        account,
        nai,
        balance,
        min_balance,
        max_balance,
        source_op_block,
        updated_at,
        DATE_TRUNC('year', updated_at) AS by_year
    FROM balance_history_by_month 
),

get_latest_updates AS (
    SELECT
        account,
        nai,
        balance,
        source_op_block,
        by_year,
        ROW_NUMBER() OVER (PARTITION BY account, nai, by_year ORDER BY updated_at DESC) AS rn_by_year
    FROM get_year
),

get_min_max_balances_by_year AS (
    SELECT
        account,
        nai,
        by_year,
        MAX(max_balance) AS max_balance,
        MIN(min_balance) AS min_balance
    FROM get_year
    GROUP BY account, nai, by_year
)

SELECT
    gl.account,
    gl.nai,
    gl.balance,
    gm.min_balance,
    gm.max_balance,
    gl.source_op_block,
    gl.by_year AS updated_at
FROM get_latest_updates gl
JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
WHERE gl.rn_by_year = 1;

CREATE OR REPLACE FUNCTION get_balance_history_aggregation(
    _account_id INT,
    _coin_type INT,
    _granularity btracker_endpoints.granularity,
    _direction btracker_endpoints.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF balance_history_aggregation -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF _from_block IS NULL THEN
    _from_block := 1;
  END IF;

  IF _to_block IS NULL THEN
    _to_block := (SELECT b.num FROM hive.blocks_view b ORDER BY b.num DESC LIMIT 1);
  END IF;

  IF _granularity = 'daily' THEN
    RETURN QUERY (
      SELECT 
        fb.date,
        fb.balance,
        fb.prev_balance,
        fb.min_balance,
        fb.max_balance
      FROM get_balance_history_by_day(
        _account_id,
        _coin_type,
        _direction,
        _from_block,
        _to_block
      ) fb
    );
  ELSEIF _granularity = 'monthly' THEN
    RETURN QUERY (
      SELECT 
        fb.date,
        fb.balance,
        fb.prev_balance,
        fb.min_balance,
        fb.max_balance
      FROM get_balance_history_by_month(
        _account_id,
        _coin_type,
        _direction,
        _from_block,
        _to_block
      ) fb
    );
  ELSEIF _granularity = 'yearly' THEN
    RETURN QUERY (
      SELECT 
        fb.date,
        fb.balance,
        fb.prev_balance,
        fb.min_balance,
        fb.max_balance
      FROM get_balance_history_by_year(
        _account_id,
        _coin_type,
        _direction,
        _from_block,
        _to_block
      ) fb
    );
  ELSE
    RAISE EXCEPTION 'Unknown granularity: %', _granularity;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION get_balance_history_by_day(
    _account_id INT,
    _coin_type INT,
    _direction btracker_endpoints.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF balance_history_aggregation -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _from_timestamp TIMESTAMP := DATE_TRUNC('day',(SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from_block)::TIMESTAMP);
  _to_timestamp TIMESTAMP := DATE_TRUNC('day', (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to_block)::TIMESTAMP);
BEGIN

RETURN QUERY (
  WITH date_series AS (
    SELECT generate_series(_from_timestamp, _to_timestamp, '1 day') AS date
  ),
  add_row_num_to_series AS (
    SELECT
      date,
      ROW_NUMBER() OVER (ORDER BY date) AS row_num
    FROM date_series
  ),
  balance_records AS (
    SELECT 
      ds.date,
      ds.row_num,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.source_op_block
    FROM add_row_num_to_series ds
    LEFT JOIN balance_history_by_day bh ON 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND 
      ds.date = bh.updated_at
  ),
  filled_balances AS (
    WITH RECURSIVE agg_history AS (
      SELECT 
        ds.date,
        ds.row_num,
        COALESCE(ds.balance, prev_balance.balance, 0) AS balance,
        COALESCE(prev_balance.balance, 0) AS prev_balance,
        COALESCE(ds.min_balance, prev_balance.min_balance, 0) AS min_balance,
        COALESCE(ds.max_balance, prev_balance.max_balance, 0) AS max_balance,
        COALESCE(ds.source_op_block, prev_balance.source_op_block, NULL) AS source_op_block
      FROM balance_records ds
      LEFT JOIN LATERAL (
        SELECT 
          bh.balance,
          bh.min_balance,
          bh.max_balance,
          bh.source_op_block 
        FROM balance_history_by_day bh
        WHERE 
          bh.account = _account_id AND 
          bh.nai = _coin_type AND 
          bh.updated_at < ds.date
        ORDER BY bh.updated_at DESC
        LIMIT 1
      ) prev_balance ON TRUE
      WHERE ds.row_num = 1
    
      UNION ALL

      SELECT 
        next_b.date,
        next_b.row_num,
        COALESCE(next_b.balance, prev_b.balance, 0) AS balance,
        COALESCE(prev_b.balance, 0) AS prev_balance,
        COALESCE(next_b.min_balance, prev_b.min_balance, 0) AS min_balance,
        COALESCE(next_b.max_balance, prev_b.max_balance, 0) AS max_balance,
        COALESCE(next_b.source_op_block, prev_b.source_op_block, NULL) AS source_op_block
      FROM agg_history prev_b
      JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
    )
    SELECT * FROM agg_history
  )
  SELECT 
    LEAST(fb.date + INTERVAL '1 day' - INTERVAL '1 second', CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
    fb.balance,
    fb.prev_balance,
    fb.min_balance,
    fb.max_balance
  FROM filled_balances fb
  ORDER BY
    (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
    (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
);
END
$$;

CREATE OR REPLACE FUNCTION get_balance_history_by_month(
    _account_id INT,
    _coin_type INT,
    _direction btracker_endpoints.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF balance_history_aggregation -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _from_timestamp TIMESTAMP := DATE_TRUNC('month',(SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from_block)::TIMESTAMP);
  _to_timestamp TIMESTAMP := DATE_TRUNC('month', (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to_block)::TIMESTAMP);
BEGIN
RETURN QUERY (
  WITH date_series AS (
    SELECT generate_series(_from_timestamp, _to_timestamp, '1 month') AS date
  ),
  add_row_num_to_series AS (
    SELECT
      date,
      ROW_NUMBER() OVER (ORDER BY date) AS row_num
    FROM date_series
  ),
  balance_records AS (
    SELECT 
      ds.date,
      ds.row_num,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.source_op_block
    FROM add_row_num_to_series ds
    LEFT JOIN balance_history_by_month bh ON 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND 
      ds.date = bh.updated_at
  ),
  filled_balances AS (
    WITH RECURSIVE agg_history AS (
      SELECT 
        ds.date,
        ds.row_num,
        COALESCE(ds.balance, prev_balance.balance, 0) AS balance,
        COALESCE(prev_balance.balance, 0) AS prev_balance,
        COALESCE(ds.min_balance, prev_balance.min_balance, 0) AS min_balance,
        COALESCE(ds.max_balance, prev_balance.max_balance, 0) AS max_balance,
        COALESCE(ds.source_op_block, prev_balance.source_op_block, NULL) AS source_op_block
      FROM balance_records ds
      LEFT JOIN LATERAL (
        SELECT 
          bh.balance,
          bh.min_balance,
          bh.max_balance,
          bh.source_op_block 
        FROM balance_history_by_month bh
        WHERE 
          bh.account = _account_id AND 
          bh.nai = _coin_type AND 
          bh.updated_at < ds.date
        ORDER BY bh.updated_at DESC
        LIMIT 1
      ) prev_balance ON TRUE
      WHERE ds.row_num = 1
    
      UNION ALL

      SELECT 
        next_b.date,
        next_b.row_num,
        COALESCE(next_b.balance, prev_b.balance, 0) AS balance,
        COALESCE(prev_b.balance, 0) AS prev_balance,
        COALESCE(next_b.min_balance, prev_b.min_balance, 0) AS min_balance,
        COALESCE(next_b.max_balance, prev_b.max_balance, 0) AS max_balance,
        COALESCE(next_b.source_op_block, prev_b.source_op_block, NULL) AS source_op_block
      FROM agg_history prev_b
      JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
    )
    SELECT * FROM agg_history
  )
  SELECT 
    LEAST(fb.date + INTERVAL '1 month' - INTERVAL '1 second', CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
    fb.balance,
    fb.prev_balance,
    fb.min_balance,
    fb.max_balance
  FROM filled_balances fb
  ORDER BY
    (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
    (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
);
END
$$;

CREATE OR REPLACE FUNCTION get_balance_history_by_year(
    _account_id INT,
    _coin_type INT,
    _direction btracker_endpoints.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF balance_history_aggregation -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _from_timestamp TIMESTAMP := DATE_TRUNC('year',(SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from_block)::TIMESTAMP);
  _to_timestamp TIMESTAMP := DATE_TRUNC('year', (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to_block)::TIMESTAMP);
BEGIN
RETURN QUERY (
  WITH date_series AS (
    SELECT generate_series(_from_timestamp, _to_timestamp, '1 year') AS date
  ),
  add_row_num_to_series AS (
    SELECT
      date,
      ROW_NUMBER() OVER (ORDER BY date) AS row_num
    FROM date_series
  ),
  balance_records AS (
    SELECT 
      ds.date,
      ds.row_num,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.source_op_block
    FROM add_row_num_to_series ds
    LEFT JOIN balance_history_by_year bh ON 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND 
      ds.date = bh.updated_at
  ),
  filled_balances AS (
    WITH RECURSIVE agg_history AS (
      SELECT 
        ds.date,
        ds.row_num,
        COALESCE(ds.balance, prev_balance.balance, 0) AS balance,
        COALESCE(prev_balance.balance, 0) AS prev_balance,
        COALESCE(ds.min_balance, prev_balance.min_balance, 0) AS min_balance,
        COALESCE(ds.max_balance, prev_balance.max_balance, 0) AS max_balance,
        COALESCE(ds.source_op_block, prev_balance.source_op_block, NULL) AS source_op_block
      FROM balance_records ds
      LEFT JOIN LATERAL (
        SELECT 
          bh.balance,
          bh.min_balance,
          bh.max_balance,
          bh.source_op_block 
        FROM balance_history_by_year bh
        WHERE 
          bh.account = _account_id AND 
          bh.nai = _coin_type AND 
          bh.updated_at < ds.date
        ORDER BY bh.updated_at DESC
        LIMIT 1
      ) prev_balance ON TRUE
      WHERE ds.row_num = 1
    
      UNION ALL

      SELECT 
        next_b.date,
        next_b.row_num,
        COALESCE(next_b.balance, prev_b.balance, 0) AS balance,
        COALESCE(prev_b.balance, 0) AS prev_balance,
        COALESCE(next_b.min_balance, prev_b.min_balance, 0) AS min_balance,
        COALESCE(next_b.max_balance, prev_b.max_balance, 0) AS max_balance,
        COALESCE(next_b.source_op_block, prev_b.source_op_block, NULL) AS source_op_block
      FROM agg_history prev_b
      JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
    )
    SELECT * FROM agg_history
  )
  SELECT 
    LEAST(fb.date + INTERVAL '1 year' - INTERVAL '1 second', CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
    fb.balance,
    fb.prev_balance,
    fb.min_balance,
    fb.max_balance
  FROM filled_balances fb
  ORDER BY
    (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
    (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
);
END
$$;

RESET ROLE;
