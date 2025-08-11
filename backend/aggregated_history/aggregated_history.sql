-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

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
    __ah_range btracker_backend.aggregated_history_range_return;
    __granularity TEXT;
    __one_period INTERVAL;

    __btracker_current_block INT := (SELECT current_block_num FROM hafd.contexts WHERE name = '%s');
  BEGIN
    __granularity := (
      CASE
        WHEN _granularity = 'daily' THEN 'day'
        WHEN _granularity = 'monthly' THEN 'month'
        WHEN _granularity = 'yearly' THEN 'year'
        ELSE NULL
      END
    );

    __ah_range := btracker_backend.aggregated_history_block_range(_from_block, _to_block, __btracker_current_block);

    __one_period := ('1 ' || __granularity )::INTERVAL;

    RETURN QUERY (
      WITH date_series AS (
        SELECT
          generate_series(
            __ah_range.from_timestamp,
            __ah_range.to_timestamp,
            __one_period
          ) AS date
      ),
      add_row_num_to_series AS (
        SELECT
          date,
          ROW_NUMBER() OVER (ORDER BY date) AS row_num
        FROM date_series
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
          sa.max_balance AS max_savings_balance
        FROM add_row_num_to_series ds
        LEFT JOIN LATERAL (
          SELECT * FROM btracker_backend.balance_history(
            _account_id,
            _coin_type,
            _granularity,
            'balance',
            __ah_range.from_block,
            __ah_range.to_block
          )
        ) bh ON ds.date = bh.updated_at
        LEFT JOIN LATERAL (
          SELECT * FROM btracker_backend.balance_history(
            _account_id,
            _coin_type,
            _granularity,
            'savings_balance',
            __ah_range.from_block,
            __ah_range.to_block
          )
        ) sa ON ds.date = sa.updated_at
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
            COALESCE(ds.max_savings_balance, ds.savings_balance, savings_prev_balance.balance, 0) AS max_savings_balance
          FROM balance_records ds
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
              __ah_range.from_block
            ) bh
          ) prev_balance ON TRUE
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
              __ah_range.from_block
            ) bh
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
            COALESCE(next_b.max_savings_balance, next_b.savings_balance, prev_b.savings_balance, 0) AS max_savings_balance
          FROM agg_history prev_b
          JOIN balance_records next_b ON next_b.row_num = prev_b.row_num + 1
        )
        SELECT * FROM agg_history
      )
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
