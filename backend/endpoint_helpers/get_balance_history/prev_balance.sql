SET ROLE btracker_owner;

-- Fallback previous-balance helper for btracker_backend.balance_history().

-- Fallback previous balance when LAG() yields NULL (first row of the first page).
-- Reads the BASE table directly for an index-only (account, nai, balance_seq_no) scan.
CREATE OR REPLACE FUNCTION btracker_backend.prev_balance(
    _balance_type btracker_backend.balance_type,
    _prev_balance BIGINT,
    _account_id   INT,
    _coin_type    INT,
    _from_seq     INT
)
RETURNS BIGINT
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  -- Optimization: if LAG() already produced a value, use it.
  IF _prev_balance IS NOT NULL THEN
    RETURN _prev_balance;
  END IF;

  IF _balance_type = 'balance' THEN
    _prev_balance := (
      SELECT abh.balance
      FROM account_balance_history abh
      WHERE abh.account = _account_id AND abh.nai = _coin_type AND abh.balance_seq_no < _from_seq
      ORDER BY abh.balance_seq_no DESC
      LIMIT 1
    );
  ELSE
    _prev_balance := (
      SELECT abh.balance
      FROM account_savings_history abh
      WHERE abh.account = _account_id AND abh.nai = _coin_type AND abh.balance_seq_no < _from_seq
      ORDER BY abh.balance_seq_no DESC
      LIMIT 1
    );
  END IF;

  -- 0 if this is the account's first balance change for this type.
  RETURN COALESCE(_prev_balance, 0);
END
$$;

RESET ROLE;
