SET ROLE btracker_owner;

-- Totals helpers (fast COUNTs)
DROP FUNCTION IF EXISTS btracker_backend.top_holders_total_balance;
CREATE OR REPLACE FUNCTION btracker_backend.top_holders_total_balance(
  _coin_type_id INT
)
RETURNS INT
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE((
    SELECT COUNT(*)
    FROM btracker_backend.current_account_balances_view
    WHERE nai = _coin_type_id AND balance > 0
  ), 0)::int;
$$;

DROP FUNCTION IF EXISTS btracker_backend.top_holders_total_savings;
CREATE OR REPLACE FUNCTION btracker_backend.top_holders_total_savings(
  _coin_type_id INT
)
RETURNS INT
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE((
    SELECT COUNT(*)
    FROM btracker_backend.account_savings_view
    WHERE nai = _coin_type_id AND balance > 0
  ), 0)::int;
$$;

-- Envelope (balance-history style) that delegates row fetch to your existing function
DROP FUNCTION IF EXISTS btracker_backend.top_holders_envelope;
CREATE OR REPLACE FUNCTION btracker_backend.top_holders_envelope(
  _coin_type_id INT,
  _balance_type btracker_backend.balance_type,
  _page        INT,
  _page_size   INT
)
RETURNS btracker_backend.top_holders
LANGUAGE plpgsql
STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
  _safe_page    INT := GREATEST(COALESCE(_page, 1), 1);
  _safe_ps      INT := GREATEST(COALESCE(_page_size, 100), 1);
  _total        INT := 0;
  _total_pages  INT := 0;
  _rows         btracker_backend.ranked_holder[];
BEGIN
  -- Totals
  IF _balance_type = 'balance' THEN
    _total := btracker_backend.top_holders_total_balance(_coin_type_id);
  ELSE
    _total := btracker_backend.top_holders_total_savings(_coin_type_id);
  END IF;

  -- Pages
  IF _total = 0 THEN
    _total_pages := 0;
    _safe_page := 1;
  ELSE
    _total_pages := CEIL(_total::numeric / _safe_ps)::int;
    _safe_page   := LEAST(_safe_page, _total_pages);
  END IF;

  -- Fetch page rows from your existing function and pack into array
  SELECT COALESCE(
           array_agg(
             (ROW(r.rank, r.account, r.value::text))::btracker_backend.ranked_holder
             ORDER BY r.rank
           ),
           ARRAY[]::btracker_backend.ranked_holder[]
         )
    INTO _rows
  FROM btracker_backend.get_top_holders(
         _coin_type_id,
         _balance_type,
         _safe_page,
         _safe_ps
       ) AS r;

  RETURN (
    _total,
    _total_pages,
    _rows
  )::btracker_backend.top_holders;
END;
$$;

RESET ROLE;
