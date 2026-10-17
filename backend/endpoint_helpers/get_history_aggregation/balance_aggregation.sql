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
3. Forward-fills missing balance values with window functions
4. Returns prev_balance for each period to show change between periods

Forward fill: a running count of non-NULL balances numbers each run of periods
that starts at a recorded balance; first_value() over that run carries the
recorded balance through the empty periods after it. Periods before the first
record carry the balance from before the range (balance_history_last_record()).
prev_balance is the previous period's filled balance (LAG). This is O(n log n) in
the number of periods; the RECURSIVE CTE it replaces re-joined the whole series
once per period.

Reads the current synced block via btracker_backend.last_synced_block().
*/
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
  __ah_range btracker_backend.aggregated_history_paging_return;
  __granularity TEXT;
  __one_period INTERVAL;

  -- Get current block from HAF context for range validation
  __btracker_current_block INT := btracker_backend.last_synced_block();
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
      -- add_row_num_to_series: Assign sequential row numbers that order the forward fill
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
      -- seed_balances: last known liquid and savings balances BEFORE the range starts,
      -- carried into the periods that precede the first record in the range
      seed_balances AS (
        SELECT
          prev_balance.balance,
          savings_prev_balance.balance AS savings_balance
        FROM btracker_backend.balance_history_last_record(
          _account_id,
          _coin_type,
          _granularity,
          'balance',
          __ah_range.from_timestamp
        ) prev_balance
        CROSS JOIN btracker_backend.balance_history_last_record(
          _account_id,
          _coin_type,
          _granularity,
          'savings_balance',
          __ah_range.from_timestamp
        ) savings_prev_balance
      ),
      -- balance_groups: each recorded balance opens a new group that the empty
      -- periods after it belong to
      balance_groups AS (
        SELECT
          br.*,
          COUNT(br.balance) OVER (ORDER BY br.row_num) AS balance_grp,
          COUNT(br.savings_balance) OVER (ORDER BY br.row_num) AS savings_grp
        FROM balance_records br
      ),
      -- carried_balances: every period's balance is its group's recorded balance;
      -- periods before the first record (group 0) carry the seed
      carried_balances AS (
        SELECT
          bg.date,
          bg.row_num,
          bg.min_balance,
          bg.max_balance,
          bg.min_savings_balance,
          bg.max_savings_balance,
          COALESCE(
            FIRST_VALUE(bg.balance) OVER (PARTITION BY bg.balance_grp ORDER BY bg.row_num),
            sb.balance,
            0
          ) AS balance,
          COALESCE(
            FIRST_VALUE(bg.savings_balance) OVER (PARTITION BY bg.savings_grp ORDER BY bg.row_num),
            sb.savings_balance,
            0
          ) AS savings_balance,
          COALESCE(sb.balance, 0) AS seed_balance,
          COALESCE(sb.savings_balance, 0) AS seed_savings_balance
        FROM balance_groups bg
        CROSS JOIN seed_balances sb
      ),
      -- filled_balances: prev_* is the previous period's carried balance (the seed for
      -- the first period); min/max fall back to the carried balance in empty periods
      filled_balances AS (
        SELECT
          cb.date,
          cb.row_num,
          cb.balance,
          COALESCE(LAG(cb.balance) OVER (ORDER BY cb.row_num), cb.seed_balance) AS prev_balance,
          COALESCE(cb.min_balance, cb.balance) AS min_balance,
          COALESCE(cb.max_balance, cb.balance) AS max_balance,

          cb.savings_balance,
          COALESCE(LAG(cb.savings_balance) OVER (ORDER BY cb.row_num), cb.seed_savings_balance) AS prev_savings_balance,
          COALESCE(cb.min_savings_balance, cb.savings_balance) AS min_savings_balance,
          COALESCE(cb.max_savings_balance, cb.savings_balance) AS max_savings_balance
        FROM carried_balances cb
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
$$;

RESET ROLE;
