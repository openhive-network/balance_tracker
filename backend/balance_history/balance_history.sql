SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.balance_history(
    _account_id INT,
    _coin_type INT,
    _balance_type btracker_backend.balance_type,
    _page INT,
    _page_size INT,
    _order_is btracker_backend.sort_direction,
    _from_block INT,
    _to_block INT
)
RETURNS btracker_backend.operation_history
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  IF _balance_type = 'balance' THEN
    RETURN btracker_backend.liquid_balance_history(_account_id, _coin_type, _page, _page_size, _order_is, _from_block, _to_block);
  END IF;

  RETURN btracker_backend.savings_history(_account_id, _coin_type, _page, _page_size, _order_is, _from_block, _to_block);
END
$$;

RESET ROLE;
