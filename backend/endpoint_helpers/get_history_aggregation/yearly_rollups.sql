-- Yearly balance-history rollups: re-aggregate per-month liquid and savings
-- balances into yearly summaries for the get_history_aggregation endpoint.

SET ROLE btracker_owner;

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

RESET ROLE;
