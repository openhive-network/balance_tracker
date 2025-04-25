-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.balance_history_by_year_return CASCADE;
CREATE TYPE btracker_backend.balance_history_by_year_return AS (
    account INT,
    nai INT,
    balance BIGINT,
    min_balance BIGINT,
    max_balance BIGINT,
    source_op_block INT,
    updated_at TIMESTAMP
);

CREATE OR REPLACE FUNCTION btracker_backend.balance_history_by_year(
    _account_id INT,
    _coin_type INT
)
RETURNS SETOF btracker_backend.balance_history_by_year_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY (
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
        WHERE account = _account_id AND nai = _coin_type 
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
    WHERE gl.rn_by_year = 1
  );

END
$$;

DROP TYPE IF EXISTS btracker_backend.saving_history_by_year_return CASCADE;
CREATE TYPE btracker_backend.saving_history_by_year_return AS (
    account INT,
    nai INT,
    balance BIGINT,
    min_balance BIGINT,
    max_balance BIGINT,
    source_op_block INT,
    updated_at TIMESTAMP
);

CREATE OR REPLACE FUNCTION btracker_backend.saving_history_by_year(
    _account_id INT,
    _coin_type INT
)
RETURNS SETOF btracker_backend.saving_history_by_year_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY (
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
        FROM saving_history_by_month
        WHERE account = _account_id AND nai = _coin_type 
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
    WHERE gl.rn_by_year = 1
  );

END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.get_balance_history_aggregation(
    _account_id INT,
    _coin_type INT,
    _granularity btracker_backend.granularity,
    _direction btracker_backend.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS SETOF btracker_backend.aggregated_history -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _from_timestamp TIMESTAMP;
  _to_timestamp TIMESTAMP;
  __granularity TEXT;
  __one_period INTERVAL;
BEGIN
  __granularity := (
    CASE 
      WHEN _granularity = 'daily' THEN 'day'
      WHEN _granularity = 'monthly' THEN 'month'
      WHEN _granularity = 'yearly' THEN 'year'
      ELSE NULL
    END
  );

  _from_timestamp := (
    CASE 
      WHEN _from_block IS NULL THEN
        DATE_TRUNC(__granularity, (SELECT b.created_at FROM hive.blocks_view b ORDER BY b.num ASC LIMIT 1)::TIMESTAMP)
      ELSE
        DATE_TRUNC(__granularity, (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from_block)::TIMESTAMP)
    END
  );

  _to_timestamp := (
    CASE 
      WHEN _to_block IS NULL THEN
        DATE_TRUNC(__granularity, (SELECT b.created_at FROM hive.blocks_view b ORDER BY b.num DESC LIMIT 1)::TIMESTAMP)
      ELSE
        DATE_TRUNC(__granularity, (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to_block)::TIMESTAMP)
    END
  );

  __one_period := ('1 ' || __granularity )::INTERVAL;

  RETURN QUERY (
    WITH date_series AS (
      SELECT 
        generate_series(
          _from_timestamp,
          _to_timestamp,
          __one_period
        ) AS date
    ),
    add_row_num_to_series AS (
      SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) AS row_num
      FROM date_series
    ),
    get_balance_aggregation AS MATERIALIZED (
      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM btracker_backend.balance_history_by_year(_account_id,_coin_type) bh
      WHERE _granularity = 'yearly'

      UNION ALL

      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM balance_history_by_day bh
      WHERE 
        bh.account = _account_id AND 
        bh.nai = _coin_type AND
        _granularity = 'daily'

      UNION ALL

      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM balance_history_by_month bh
      WHERE 
        bh.account = _account_id AND 
        bh.nai = _coin_type AND
        _granularity = 'monthly'

    ),
    get_savings_aggregation AS MATERIALIZED (
      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM btracker_backend.saving_history_by_year(_account_id,_coin_type) bh
      WHERE _granularity = 'yearly'

      UNION ALL

      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM saving_history_by_day bh
      WHERE 
        bh.account = _account_id AND 
        bh.nai = _coin_type AND
        _granularity = 'daily'

      UNION ALL

      SELECT 
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.source_op_block,
        bh.updated_at
      FROM saving_history_by_month bh
      WHERE 
        bh.account = _account_id AND 
        bh.nai = _coin_type AND
        _granularity = 'monthly'
    ),
    balance_records AS MATERIALIZED (
      SELECT 
        ds.date,
        ds.row_num,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        sa.balance AS savings_balance,
        sa.min_balance AS min_savings_balance,
        sa.max_balance AS max_savings_balance,
        bh.source_op_block
      FROM add_row_num_to_series ds
      LEFT JOIN get_balance_aggregation bh ON ds.date = bh.updated_at
      LEFT JOIN get_savings_aggregation sa ON ds.date = sa.updated_at
    ),
    filled_balances AS (
      WITH RECURSIVE agg_history AS (
        SELECT 
          ds.date,
          ds.row_num,
          COALESCE(ds.balance, prev_balance.balance, 0) AS balance,
          COALESCE(prev_balance.balance, 0) AS prev_balance,
          COALESCE(ds.min_balance, ds.balance, prev_balance.balance, 0) AS min_balance,
          COALESCE(ds.max_balance, ds.balance, prev_balance.balance, 0) AS max_balance,

          COALESCE(ds.savings_balance, savings_prev_balance.balance, 0) AS savings_balance,
          COALESCE(savings_prev_balance.balance, 0) AS prev_savings_balance,
          COALESCE(ds.min_savings_balance, ds.savings_balance, savings_prev_balance.balance, 0) AS min_savings_balance,
          COALESCE(ds.max_savings_balance, ds.savings_balance, savings_prev_balance.balance, 0) AS max_savings_balance,

          COALESCE(ds.source_op_block, prev_balance.source_op_block, NULL) AS source_op_block
        FROM balance_records ds
        LEFT JOIN LATERAL (
          SELECT 
            bh.balance,
            bh.min_balance,
            bh.max_balance,
            bh.source_op_block 
          FROM get_balance_aggregation bh
          WHERE bh.updated_at < ds.date
          ORDER BY bh.updated_at DESC
          LIMIT 1
        ) prev_balance ON TRUE
        LEFT JOIN LATERAL (
          SELECT 
            bh.balance,
            bh.min_balance,
            bh.max_balance,
            bh.source_op_block 
          FROM get_savings_aggregation bh
          WHERE bh.updated_at < ds.date
          ORDER BY bh.updated_at DESC
          LIMIT 1
        ) savings_prev_balance ON TRUE
        WHERE ds.row_num = 1
      
        UNION ALL

        SELECT 
          next_b.date,
          next_b.row_num,
          COALESCE(next_b.balance, prev_b.balance, 0) AS balance,
          COALESCE(prev_b.balance, 0) AS prev_balance,
          COALESCE(next_b.min_balance, next_b.balance, prev_b.balance, 0) AS min_balance,
          COALESCE(next_b.max_balance, next_b.balance, prev_b.balance, 0) AS max_balance,

          COALESCE(next_b.savings_balance, prev_b.savings_balance, 0) AS savings_balance,
          COALESCE(prev_b.savings_balance, 0) AS prev_savings_balance,
          COALESCE(next_b.min_savings_balance, next_b.savings_balance, prev_b.savings_balance, 0) AS min_savings_balance,
          COALESCE(next_b.max_savings_balance, next_b.savings_balance, prev_b.savings_balance, 0) AS max_savings_balance,

          COALESCE(next_b.source_op_block, prev_b.source_op_block, NULL) AS source_op_block
        FROM agg_history prev_b
        JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
      )
      SELECT * FROM agg_history
    )
    SELECT 
      LEAST(fb.date + __one_period - INTERVAL '1 second', CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
      (
        fb.balance::TEXT,
        fb.savings_balance::TEXT
      )::btracker_backend.balances AS balance,
      (
        fb.prev_balance::TEXT,
        fb.prev_savings_balance::TEXT
      )::btracker_backend.balances AS prev_balance,
      (
        fb.min_balance::TEXT,
        fb.min_savings_balance::TEXT
      )::btracker_backend.balances AS min_balance,
      (
        fb.max_balance::TEXT,
        fb.max_savings_balance::TEXT
      )::btracker_backend.balances AS max_balance
    FROM filled_balances fb
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN fb.date ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc' THEN fb.date ELSE NULL END) ASC
  );

END
$$;

RESET ROLE;
