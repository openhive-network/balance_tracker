-- noqa: disable=AL01, AM05

-- Per-granularity balance history readers: return type and routers that
-- dispatch to the daily/monthly/yearly source for liquid and savings balances.

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
