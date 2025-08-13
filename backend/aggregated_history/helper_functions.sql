-- noqa: disable=AL01, AM05

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.balance_history_return CASCADE;
CREATE TYPE btracker_backend.balance_history_return AS (
    account     INT,
    nai         SMALLINT,
    balance     BIGINT,
    min_balance BIGINT,
    max_balance BIGINT,
    updated_at  TIMESTAMP
);

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

    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
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
        gl.by_year AS updated_at
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
  );

END
$$;

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

    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
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

    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
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
        gl.by_year AS updated_at
    FROM get_latest_updates gl
    JOIN get_min_max_balances_by_year gm ON gl.account = gm.account AND gl.nai = gm.nai AND gl.by_year = gm.by_year
    WHERE gl.rn_by_year = 1
  );

END
$$;

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

    get_latest_updates AS (
        SELECT
            account,
            nai,
            balance,
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
