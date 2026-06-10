SET ROLE btracker_owner;

/*
Returns paginated leaderboard of top asset holders for a given coin type.
Called by: btracker_endpoints.get_top_holders()

Uses idx_account_balance_nai_balance_idx or idx_account_savings_nai_balance_idx
(created after massive sync completes via btracker_app.btracker_on_end_blocks_processing).
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_top_holders(
    _coin_type    INT,
    _balance_type btracker_backend.balance_type,
    _page         INT,
    _limit        INT
)
RETURNS btracker_backend.top_holders
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _ofs          INT := (_page - 1) * _limit;
  _total        INT := 0;
  _total_pages  INT := 0;
  _rows         btracker_backend.ranked_holder[];
BEGIN
  IF _balance_type = 'balance' THEN
    -- Count total accounts with positive balance (required for pagination UI)
    SELECT COUNT(*)
      INTO _total
    FROM btracker_backend.current_account_balances_view
    WHERE nai = _coin_type AND balance > 0;

    IF _total = 0 THEN
      _total_pages := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _limit)::INT;

      -- Fetch paginated results with global ranking
      -- MATERIALIZED CTE prevents re-execution of expensive sorted query
      -- ROW_NUMBER() + offset gives global rank across all pages
      WITH ordered_holders AS MATERIALIZED (
        SELECT av.name, src.balance
        FROM btracker_backend.current_account_balances_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _ofs
        LIMIT _limit
      ),
      ranked AS (
        SELECT
          (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _ofs)::INT AS rank,
          o.name::TEXT AS account,
          -- NUMERIC(38,0) prevents overflow for very large VESTS values
          o.balance::NUMERIC(38,0) AS value
        FROM ordered_holders o
      )
      SELECT COALESCE(
               array_agg(
                 (ROW(r.rank, r.account, r.value::TEXT))::btracker_backend.ranked_holder
                 ORDER BY r.rank
               ),
               ARRAY[]::btracker_backend.ranked_holder[]
             )
        INTO _rows
      FROM ranked r;
    END IF;

  ELSIF _balance_type = 'savings_balance' THEN
    -- Count total accounts with positive savings balance
    SELECT COUNT(*)
      INTO _total
    FROM btracker_backend.account_savings_view
    WHERE nai = _coin_type AND balance > 0;

    IF _total = 0 THEN
      _total_pages := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _limit)::INT;

      -- Same pattern as liquid balance branch
      WITH ordered_holders AS MATERIALIZED (
        SELECT av.name, src.balance
        FROM btracker_backend.account_savings_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _ofs
        LIMIT _limit
      ),
      ranked AS (
        SELECT
          (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _ofs)::INT AS rank,
          o.name::TEXT AS account,
          o.balance::NUMERIC(38,0) AS value
        FROM ordered_holders o
      )
      SELECT COALESCE(
               array_agg(
                 (ROW(r.rank, r.account, r.value::TEXT))::btracker_backend.ranked_holder
                 ORDER BY r.rank
               ),
               ARRAY[]::btracker_backend.ranked_holder[]
             )
        INTO _rows
      FROM ranked r;
    END IF;

  ELSE
    RAISE EXCEPTION 'Unsupported balance type: %', _balance_type;
  END IF;

  RETURN (_total, _total_pages, _rows)::btracker_backend.top_holders;
END;
$$;

RESET ROLE;
