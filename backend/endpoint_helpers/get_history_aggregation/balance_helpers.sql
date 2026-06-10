-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

-- Return type for aggregated balance history helper functions.
-- Contains balance snapshot with min/max for a time period.
DROP TYPE IF EXISTS btracker_backend.balance_history_return CASCADE;
CREATE TYPE btracker_backend.balance_history_return AS (
    account     INT,
    nai         SMALLINT,
    balance     BIGINT,
    min_balance BIGINT,
    max_balance BIGINT,
    updated_at  TIMESTAMP
);

/*
Aggregates monthly liquid balance history into yearly summaries.
Called by: btracker_backend.balance_history() when _granularity = 'yearly' and _balance_type = 'balance'

Pattern: Reads from balance_history_by_month table and re-aggregates by year.
Uses ROW_NUMBER() to get the final balance of each year (latest month's closing balance).
*/
CREATE OR REPLACE FUNCTION btracker_backend.balance_history_by_year(
    _account_id INT,
    _coin_type INT,
    _from TIMESTAMP,
    _to TIMESTAMP
)
RETURNS SETOF btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY (
    -- get_year: Extract monthly records and truncate timestamps to year boundary
    WITH get_year AS (
        SELECT
            account,
            nai,
            balance,
            min_balance,
            max_balance,
            updated_at,
            DATE_TRUNC('year', updated_at) AS by_year
        FROM balance_history_by_month
        WHERE account = _account_id AND nai = _coin_type AND
              DATE_TRUNC('year', updated_at) BETWEEN _from AND _to
    ),

    -- get_latest_updates: Find the last month of each year using ROW_NUMBER()
    -- rn_by_year = 1 means this is the latest month's record for that year
    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
            by_year,
            ROW_NUMBER() OVER (PARTITION BY account, nai, by_year ORDER BY updated_at DESC) AS rn_by_year
        FROM get_year
    ),

    -- get_min_max_balances_by_year: Aggregate min/max across all months in each year
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

    -- Join latest balance with yearly min/max aggregates
    SELECT
        gl.account,
        gl.nai,
        gl.balance,
        gm.min_balance,
        gm.max_balance,
        gl.by_year AS updated_at
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
  );

END
$$;

/*
Returns the most recent yearly liquid balance record BEFORE the specified timestamp.
Called by: btracker_backend.balance_history_last_record() when _granularity = 'yearly' and _balance_type = 'balance'

Used by get_balance_history_aggregation() to initialize the RECURSIVE CTE's base case
with the correct prev_balance value for the first period in the range.
*/
CREATE OR REPLACE FUNCTION btracker_backend.balance_history_by_year_last_record(
    _account_id INT,
    _coin_type INT,
    _from TIMESTAMP
)
RETURNS btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN (
    -- get_year: Extract monthly records BEFORE the range starts
    WITH get_year AS (
        SELECT
            account,
            nai,
            balance,
            min_balance,
            max_balance,
            updated_at,
            DATE_TRUNC('year', updated_at) AS by_year
        FROM balance_history_by_month
        WHERE account = _account_id AND nai = _coin_type AND
              updated_at < _from
    ),

    -- get_latest_updates: Find the last month of each year
    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
            by_year,
            ROW_NUMBER() OVER (PARTITION BY account, nai, by_year ORDER BY updated_at DESC) AS rn_by_year
        FROM get_year
    ),

    -- get_min_max_balances_by_year: Aggregate min/max for each year
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

    -- Return only the most recent year (ORDER BY DESC LIMIT 1)
    SELECT (
        gl.account,
        gl.nai,
        gl.balance,
        gm.min_balance,
        gm.max_balance,
        gl.by_year
    )::btracker_backend.balance_history_return
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
    ORDER BY gl.by_year DESC
    LIMIT 1
  );

END
$$;


/*
Aggregates monthly savings balance history into yearly summaries.
Called by: btracker_backend.balance_history() when _granularity = 'yearly' and _balance_type = 'savings_balance'

Pattern: Same as balance_history_by_year() but reads from saving_history_by_month table.
*/
CREATE OR REPLACE FUNCTION btracker_backend.saving_history_by_year(
    _account_id INT,
    _coin_type INT,
    _from TIMESTAMP,
    _to TIMESTAMP
)
RETURNS SETOF btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN QUERY (
    -- get_year: Extract monthly savings records and truncate to year
    WITH get_year AS (
        SELECT
            account,
            nai,
            balance,
            min_balance,
            max_balance,
            updated_at,
            DATE_TRUNC('year', updated_at) AS by_year
        FROM saving_history_by_month
        WHERE account = _account_id AND nai = _coin_type AND
              DATE_TRUNC('year', updated_at) BETWEEN _from AND _to
    ),

    -- get_latest_updates: Find the last month of each year
    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
            by_year,
            ROW_NUMBER() OVER (PARTITION BY account, nai, by_year ORDER BY updated_at DESC) AS rn_by_year
        FROM get_year
    ),

    -- get_min_max_balances_by_year: Aggregate min/max across all months
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

    -- Join latest balance with yearly min/max aggregates
    SELECT
        gl.account,
        gl.nai,
        gl.balance,
        gm.min_balance,
        gm.max_balance,
        gl.by_year AS updated_at
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
  );

END
$$;

/*
Returns the most recent yearly savings balance record BEFORE the specified timestamp.
Called by: btracker_backend.balance_history_last_record() when _granularity = 'yearly' and _balance_type = 'savings_balance'

Pattern: Same as balance_history_by_year_last_record() but reads from saving_history_by_month table.
*/
CREATE OR REPLACE FUNCTION btracker_backend.saving_history_by_year_last_record(
    _account_id INT,
    _coin_type INT,
    _from TIMESTAMP
)
RETURNS btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  RETURN (
    -- get_year: Extract monthly savings records BEFORE the range
    WITH get_year AS (
        SELECT
            account,
            nai,
            balance,
            min_balance,
            max_balance,
            updated_at,
            DATE_TRUNC('year', updated_at) AS by_year
        FROM saving_history_by_month
        WHERE account = _account_id AND nai = _coin_type AND
              updated_at < _from
    ),

    -- get_latest_updates: Find the last month of each year
    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
            by_year,
            ROW_NUMBER() OVER (PARTITION BY account, nai, by_year ORDER BY updated_at DESC) AS rn_by_year
        FROM get_year
    ),

    -- get_min_max_balances_by_year: Aggregate min/max for each year
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

    -- Return only the most recent year
    SELECT (
        gl.account,
        gl.nai,
        gl.balance,
        gm.min_balance,
        gm.max_balance,
        gl.by_year
    )::btracker_backend.balance_history_return
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
    ORDER BY gl.by_year DESC
    LIMIT 1
  );

END
$$;

/*
Router function that dispatches to the appropriate balance history table based on granularity and balance type.
Called by: btracker_backend.get_balance_history_aggregation() to fetch raw balance data

Routes to:
  - balance + yearly:  btracker_backend.balance_history_by_year()
  - balance + daily:   balance_history_by_day table
  - balance + monthly: balance_history_by_month table
  - savings_balance + yearly:  btracker_backend.saving_history_by_year()
  - savings_balance + daily:   saving_history_by_day table
  - savings_balance + monthly: saving_history_by_month table
*/
CREATE OR REPLACE FUNCTION btracker_backend.balance_history(
    _account_id INT,
    _coin_type INT,
    _granularity btracker_backend.granularity,
    _balance_type btracker_backend.balance_type,
    _from TIMESTAMP,
    _to TIMESTAMP
)
RETURNS SETOF btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  -- Route based on balance_type and granularity combination
  IF _balance_type = 'balance' AND _granularity = 'yearly' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM btracker_backend.balance_history_by_year(_account_id, _coin_type, _from, _to) bh;

  ELSEIF _balance_type = 'balance' AND _granularity = 'daily' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM balance_history_by_day bh
      WHERE bh.account = _account_id
        AND bh.nai     = _coin_type
        AND bh.updated_at BETWEEN _from AND _to;

  ELSEIF _balance_type = 'balance' AND _granularity = 'monthly' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM balance_history_by_month bh
      WHERE bh.account = _account_id
        AND bh.nai = _coin_type
        AND bh.updated_at BETWEEN _from AND _to;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'yearly' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM btracker_backend.saving_history_by_year(_account_id, _coin_type, _from, _to) bh;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'daily' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM saving_history_by_day bh
      WHERE bh.account = _account_id
        AND bh.nai = _coin_type
        AND bh.updated_at BETWEEN _from AND _to;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'monthly' THEN
    RETURN QUERY
      SELECT
        bh.account,
        bh.nai,
        bh.balance,
        bh.min_balance,
        bh.max_balance,
        bh.updated_at
      FROM saving_history_by_month bh
      WHERE bh.account = _account_id
        AND bh.nai = _coin_type
        AND bh.updated_at BETWEEN _from AND _to;

  ELSE
    RAISE EXCEPTION 'Invalid granularity: %, balance-type: %', _granularity, _balance_type;
  END IF;
END
$$;

/*
Router function that returns the most recent balance record BEFORE the specified timestamp.
Called by: btracker_backend.get_balance_history_aggregation() to get prev_balance for RECURSIVE CTE base case

Routes to:
  - balance + yearly:  btracker_backend.balance_history_by_year_last_record()
  - balance + daily:   balance_history_by_day table (ORDER BY DESC LIMIT 1)
  - balance + monthly: balance_history_by_month table (ORDER BY DESC LIMIT 1)
  - savings_balance + yearly:  btracker_backend.saving_history_by_year_last_record()
  - savings_balance + daily:   saving_history_by_day table (ORDER BY DESC LIMIT 1)
  - savings_balance + monthly: saving_history_by_month table (ORDER BY DESC LIMIT 1)
*/
CREATE OR REPLACE FUNCTION btracker_backend.balance_history_last_record(
    _account_id INT,
    _coin_type INT,
    _granularity btracker_backend.granularity,
    _balance_type btracker_backend.balance_type,
    _from TIMESTAMP
)
RETURNS btracker_backend.balance_history_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  -- Route based on balance_type and granularity combination
  IF _balance_type = 'balance' AND _granularity = 'yearly' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM btracker_backend.balance_history_by_year_last_record(_account_id, _coin_type, _from) bh;

  ELSEIF _balance_type = 'balance' AND _granularity = 'monthly' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM balance_history_by_month bh
    WHERE 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND
      bh.updated_at < _from
    ORDER BY bh.updated_at DESC
    LIMIT 1;

  ELSEIF _balance_type = 'balance' AND _granularity = 'daily' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM balance_history_by_day bh
    WHERE 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND
      bh.updated_at < _from
    ORDER BY bh.updated_at DESC
    LIMIT 1;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'yearly' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM btracker_backend.saving_history_by_year_last_record(_account_id, _coin_type, _from) bh;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'monthly' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM saving_history_by_month bh
    WHERE 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND
      bh.updated_at < _from 
    ORDER BY bh.updated_at DESC
    LIMIT 1;

  ELSEIF _balance_type = 'savings_balance' AND _granularity = 'daily' THEN
    RETURN (
      bh.account,
      bh.nai,
      bh.balance,
      bh.min_balance,
      bh.max_balance,
      bh.updated_at
    )::btracker_backend.balance_history_return
    FROM saving_history_by_day bh
    WHERE 
      bh.account = _account_id AND 
      bh.nai = _coin_type AND
      bh.updated_at < _from 
    ORDER BY bh.updated_at DESC
    LIMIT 1;

  ELSE
    RAISE EXCEPTION 'Invalid granularity: %, balance-type: %', _granularity, _balance_type;
  END IF;
END
$$;

RESET ROLE;
