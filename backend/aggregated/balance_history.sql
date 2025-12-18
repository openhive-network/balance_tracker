-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

/*
Returns aggregated balance history with gap-filling for continuous time series.
Called by: btracker_endpoints.get_aggregated_balance()

This function provides a complete balance timeline for an account, filling in
periods where no balance changes occurred with the previous known balance.

Key features:
1. Generates continuous date series for the requested granularity (daily/monthly/yearly)
2. Fetches both liquid balance AND savings balance for the same coin type
3. Uses RECURSIVE CTE to forward-fill missing balance values
4. Returns prev_balance for each period to show change between periods

The RECURSIVE CTE pattern is essential here because:
- Simple JOIN cannot propagate values forward (only matches or NULL)
- We need row N to inherit from computed row N-1, not from source data
- Each row's prev_balance depends on the computed balance of the previous row

Uses dynamic SQL (DO block with format()) to inject the HAF context schema name,
which is required to get the current block number for range calculation.
*/
DO $$
DECLARE
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(

  $BODY$

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
  $bp$
  DECLARE
    __ah_range btracker_backend.aggregated_history_paging_return;
    __granularity TEXT;
    __one_period INTERVAL;

    -- Get current block from HAF context for range validation
    __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = '%s');
  BEGIN
    -- Convert enum granularity to PostgreSQL interval unit string
    __granularity := (
      CASE
        WHEN _granularity = 'daily' THEN 'day'
        WHEN _granularity = 'monthly' THEN 'month'
        WHEN _granularity = 'yearly' THEN 'year'
        ELSE NULL
      END
    );

    -- Convert block range to timestamp range, clamped to valid data
    __ah_range := btracker_backend.aggregated_history_block_range(_from_block, _to_block, __btracker_current_block, __granularity);

    -- Build interval for generate_series step size
    __one_period := ('1 ' || __granularity )::INTERVAL;

    RETURN QUERY (
      -- date_series: Generate continuous sequence of time buckets
      -- This ensures every period in the range is represented
      WITH date_series AS (
        SELECT
          generate_series(
            __ah_range.from_timestamp,
            __ah_range.to_timestamp,
            __one_period
          ) AS date
      ),
      -- add_row_num_to_series: Assign sequential row numbers for RECURSIVE CTE iteration
      -- The RECURSIVE CTE processes rows by row_num, linking N to N-1
      add_row_num_to_series AS (
        SELECT
          date,
          ROW_NUMBER() OVER (ORDER BY date) AS row_num
        FROM date_series
      ),
      -- balance_records: MATERIALIZED for single execution
      -- LEFT JOIN both liquid AND savings balance data onto the date series
      -- NULL values indicate no balance change in that period (to be filled later)
      balance_records AS MATERIALIZED (
        SELECT
          ds.date,
          ds.row_num,
          bh.balance,
          bh.min_balance,
          bh.max_balance,
          sa.balance AS savings_balance,
          sa.min_balance AS min_savings_balance,
          sa.max_balance AS max_savings_balance
        FROM add_row_num_to_series ds
        -- Fetch liquid balance records for this account/coin/granularity
        LEFT JOIN LATERAL (
          SELECT * FROM btracker_backend.balance_history(
            _account_id,
            _coin_type,
            _granularity,
            'balance',
            __ah_range.from_timestamp,
            __ah_range.to_timestamp
          )
        ) bh ON ds.date = bh.updated_at
        -- Fetch savings balance records for this account/coin/granularity
        LEFT JOIN LATERAL (
          SELECT * FROM btracker_backend.balance_history(
            _account_id,
            _coin_type,
            _granularity,
            'savings_balance',
            __ah_range.from_timestamp,
            __ah_range.to_timestamp
          )
        ) sa ON ds.date = sa.updated_at
      ),
      -- filled_balances: RECURSIVE CTE for forward-fill gap-filling pattern
      -- Each iteration processes the next row, inheriting balance from previous if NULL
      filled_balances AS (
        WITH RECURSIVE agg_history AS (
          -- Base case (row_num = 1): Initialize first row
          -- Use balance_history_last_record() to get the balance BEFORE the range starts
          -- This ensures we have correct prev_balance even for the first period
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
            COALESCE(ds.max_savings_balance, ds.savings_balance, savings_prev_balance.balance, 0) AS max_savings_balance
          FROM balance_records ds
          -- Lookup last known liquid balance before the range
          LEFT JOIN LATERAL (
            SELECT
              bh.balance,
              bh.min_balance,
              bh.max_balance
            FROM btracker_backend.balance_history_last_record(
              _account_id,
              _coin_type,
              _granularity,
              'balance',
              __ah_range.from_timestamp
            ) bh
          ) prev_balance ON TRUE
          -- Lookup last known savings balance before the range
          LEFT JOIN LATERAL (
            SELECT
              bh.balance,
              bh.min_balance,
              bh.max_balance
            FROM btracker_backend.balance_history_last_record(
              _account_id,
              _coin_type,
              _granularity,
              'savings_balance',
              __ah_range.from_timestamp
            ) bh
          ) savings_prev_balance ON TRUE
          WHERE ds.row_num = 1

          UNION ALL

          -- Recursive case: Process row N using computed values from row N-1
          -- Key insight: prev_b.balance is the COMPUTED balance, not source data
          -- This propagates the last known balance forward through gaps
          SELECT
            next_b.date,
            next_b.row_num,
            -- If current row has no balance data, inherit from previous computed row
            COALESCE(next_b.balance, prev_b.balance, 0) AS balance,
            COALESCE(prev_b.balance, 0) AS prev_balance,
            COALESCE(next_b.min_balance, next_b.balance, prev_b.balance, 0) AS min_balance,
            COALESCE(next_b.max_balance, next_b.balance, prev_b.balance, 0) AS max_balance,

            COALESCE(next_b.savings_balance, prev_b.savings_balance, 0) AS savings_balance,
            COALESCE(prev_b.savings_balance, 0) AS prev_savings_balance,
            COALESCE(next_b.min_savings_balance, next_b.savings_balance, prev_b.savings_balance, 0) AS min_savings_balance,
            COALESCE(next_b.max_savings_balance, next_b.savings_balance, prev_b.savings_balance, 0) AS max_savings_balance
          FROM agg_history prev_b
          JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
        )
        SELECT * FROM agg_history
      )
      -- Final output: format results into composite types for JSON serialization
      -- LEAST prevents future timestamps from exceeding current time
      SELECT
        LEAST(fb.date + __one_period, CURRENT_TIMESTAMP)::TIMESTAMP AS adjusted_date,
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
  $bp$;

  $BODY$, __schema_name, __schema_name);
END
$$;

RESET ROLE;
