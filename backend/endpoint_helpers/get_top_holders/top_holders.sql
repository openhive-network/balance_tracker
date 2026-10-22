SET ROLE btracker_owner;

/*
Returns paginated leaderboard of top asset holders for a given coin type.
Called by: btracker_endpoints.get_top_holders()

Uses idx_account_balance_nai_balance_idx or idx_account_savings_nai_balance_idx
(created after massive sync completes via btracker_app.btracker_on_end_blocks_processing).
The unfiltered liquid holder count is read from balance_totals.

The optional [_min_balance, _max_balance) range filters holders before pagination.
Bounds are half-open (balance >= _min_balance AND balance < _max_balance); a NULL bound
disables that side, so a NULL _max_balance yields an open-ended top bracket. The range is
generic and applies to whichever coin-type/balance-type is requested.

Ranking stays GLOBAL: a filtered holder keeps its position in the full leaderboard, so the
first in-range holder's rank is offset by _above, the count of holders ranked above the
upper bound (balance >= _max_balance). With no upper bound nothing ranks above the range,
so the offset is zero.

'total' (#63) ranks by liquid + savings. The two live in different tables, so no index
can order the sum: it is built once per request with a FULL JOIN, and the count, the
rank offset and the page all come from that one pass. Names are resolved only for
accounts at or above the page's cut-off total, which keeps the (total DESC, name ASC)
tie-break exact without a name lookup for every holder (~0.6 s for HIVE, ~586k
holders, on a synced node). The range filters apply to the sum.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_top_holders(
    _coin_type    INT,
    _balance_type btracker_backend.top_holders_balance_type,
    _page         INT,
    _limit        INT,
    _min_balance  BIGINT,
    _max_balance  BIGINT
)
RETURNS btracker_backend.top_holders
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _ofs          INT := (_page - 1) * _limit;
  _total        INT := 0;
  _total_pages  INT := 0;
  _above        INT := 0;  -- holders ranked above _max_balance, kept in the global rank
  _rows         btracker_backend.ranked_holder[];
BEGIN
  IF _balance_type = 'balance' THEN
    -- Count after filtering so pagination metadata describes the selected range.
    -- Unfiltered, it is the holder count balance_totals maintains.
    IF _min_balance IS NULL AND _max_balance IS NULL THEN
      SELECT COALESCE((SELECT bt.holders FROM balance_totals bt WHERE bt.nai = _coin_type), 0)
        INTO _total;
    ELSE
      SELECT COUNT(*)
        INTO _total
      FROM btracker_backend.current_account_balances_view
      WHERE nai = _coin_type
        AND balance > 0
        AND (_min_balance IS NULL OR balance >= _min_balance)
        AND (_max_balance IS NULL OR balance < _max_balance);
    END IF;

    IF _total = 0 THEN
      _total_pages := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _limit)::INT;

      -- Holders above the upper bound are filtered out of the page but still
      -- occupy global ranks, so the in-range ranks start after them.
      IF _max_balance IS NOT NULL THEN
        SELECT COUNT(*)
          INTO _above
        FROM btracker_backend.current_account_balances_view
        WHERE nai = _coin_type
          AND balance > 0
          AND balance >= _max_balance;
      END IF;

      -- Fetch paginated results with global ranking
      -- MATERIALIZED CTE prevents re-execution of expensive sorted query
      -- ROW_NUMBER() + offset gives global rank across all pages
      WITH ordered_holders AS MATERIALIZED (
        SELECT av.name, src.balance
        FROM btracker_backend.current_account_balances_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
          AND src.balance > 0
          AND (_min_balance IS NULL OR src.balance >= _min_balance)
          AND (_max_balance IS NULL OR src.balance < _max_balance)
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _ofs
        LIMIT _limit
      ),
      ranked AS (
        SELECT
          (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _ofs + _above)::INT AS rank,
          o.name::TEXT AS account,
          -- NUMERIC(38,0) holds any balance/VESTS amount without overflow
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
    -- Count after filtering so pagination metadata describes the selected range.
    SELECT COUNT(*)
      INTO _total
    FROM btracker_backend.account_savings_view
    WHERE nai = _coin_type
      AND balance > 0
      AND (_min_balance IS NULL OR balance >= _min_balance)
      AND (_max_balance IS NULL OR balance < _max_balance);

    IF _total = 0 THEN
      _total_pages := 0;
      _rows := ARRAY[]::btracker_backend.ranked_holder[];
    ELSE
      _total_pages := CEIL(_total::numeric / _limit)::INT;

      -- See the liquid branch: keep rank global across the filtered upper bound.
      IF _max_balance IS NOT NULL THEN
        SELECT COUNT(*)
          INTO _above
        FROM btracker_backend.account_savings_view
        WHERE nai = _coin_type
          AND balance > 0
          AND balance >= _max_balance;
      END IF;

      -- Same pattern as liquid balance branch
      WITH ordered_holders AS MATERIALIZED (
        SELECT av.name, src.balance
        FROM btracker_backend.account_savings_view AS src
        JOIN hive.accounts_view av ON av.id = src.account
        WHERE src.nai = _coin_type
          AND src.balance > 0
          AND (_min_balance IS NULL OR src.balance >= _min_balance)
          AND (_max_balance IS NULL OR src.balance < _max_balance)
        ORDER BY src.balance DESC, av.name ASC
        OFFSET _ofs
        LIMIT _limit
      ),
      ranked AS (
        SELECT
          (ROW_NUMBER() OVER (ORDER BY o.balance DESC, o.name ASC) + _ofs + _above)::INT AS rank,
          o.name::TEXT AS account,
          -- NUMERIC(38,0) holds any balance amount without overflow
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

  ELSIF _balance_type = 'total' THEN
    WITH sums AS MATERIALIZED (
      SELECT COALESCE(c.account, s.account) AS account,
             COALESCE(c.balance, 0) + COALESCE(s.balance, 0) AS total
      FROM (
        SELECT cab.account, cab.balance
        FROM btracker_backend.current_account_balances_view cab
        WHERE cab.nai = _coin_type
      ) c
      FULL JOIN (
        SELECT sav.account, sav.balance
        FROM btracker_backend.account_savings_view sav
        WHERE sav.nai = _coin_type
      ) s ON s.account = c.account
    ),
    in_range AS MATERIALIZED (
      SELECT su.account, su.total
      FROM sums su
      WHERE su.total > 0
        AND (_min_balance IS NULL OR su.total >= _min_balance)
        AND (_max_balance IS NULL OR su.total < _max_balance)
    ),
    -- Holders above the upper bound keep their global ranks (see the liquid branch).
    above AS (
      SELECT CASE WHEN _max_balance IS NULL THEN 0
                  ELSE (SELECT COUNT(*) FROM sums su WHERE su.total > 0 AND su.total >= _max_balance)
             END::INT AS n
    ),
    -- This page's slice of totals, without names (a top-N sort of the totals only).
    -- Every row ranked before the page has a total >= its highest total, and every row
    -- tied with the page's edges lies within [lowest, highest]; so names are needed
    -- only for that band, and the rows above it are just counted.
    page_totals AS (
      SELECT ir.total
      FROM in_range ir
      ORDER BY ir.total DESC
      OFFSET _ofs
      LIMIT _limit
    ),
    band AS (
      SELECT MAX(pt.total) AS hi, MIN(pt.total) AS lo FROM page_totals pt
    ),
    skipped AS (
      SELECT COUNT(*)::INT AS n
      FROM in_range ir, band b
      WHERE ir.total > b.hi
    ),
    ordered_holders AS MATERIALIZED (
      SELECT av.name, ir.total
      FROM in_range ir
      CROSS JOIN band b
      CROSS JOIN LATERAL (
        SELECT acc.name FROM hive.accounts_view acc WHERE acc.id = ir.account LIMIT 1
      ) av
      WHERE ir.total BETWEEN b.lo AND b.hi
      ORDER BY ir.total DESC, av.name ASC
      OFFSET _ofs - (SELECT sk.n FROM skipped sk)
      LIMIT _limit
    ),
    ranked AS (
      SELECT
        (ROW_NUMBER() OVER (ORDER BY o.total DESC, o.name ASC) + _ofs + (SELECT a.n FROM above a))::INT AS rank,
        o.name::TEXT AS account,
        o.total::NUMERIC(38,0) AS value
      FROM ordered_holders o
    )
    SELECT
      (SELECT COUNT(*) FROM in_range)::INT,
      COALESCE(
        (SELECT array_agg(
                  (ROW(r.rank, r.account, r.value::TEXT))::btracker_backend.ranked_holder
                  ORDER BY r.rank)
         FROM ranked r),
        ARRAY[]::btracker_backend.ranked_holder[]
      )
      INTO _total, _rows;

    _total_pages := CASE WHEN _total = 0 THEN 0 ELSE CEIL(_total::numeric / _limit)::INT END;

  ELSE
    RAISE EXCEPTION 'Unsupported balance type: %', _balance_type;
  END IF;

  RETURN (_total, _total_pages, _rows)::btracker_backend.top_holders;
END;
$$;

RESET ROLE;
