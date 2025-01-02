SET ROLE btracker_owner;

-- process limit orders
CREATE OR REPLACE FUNCTION process_limit_order(
  _op_id BIGINT,
  _block_num INT,
  _account INT,
  _amount BIGINT,
  _nai INT,
  _price NUMERIC,
  _order_type SMALLINT,
  _timestamp TIMESTAMP
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
  INSERT INTO account_open_orders(
    account, nai, amount, price, order_type, created_date, 
    order_id, source_op, source_op_block
  )
  VALUES (
    _account, _nai, _amount, _price, _order_type, _timestamp,
    _op_id, _op_id, _block_num
  );
END;
$$;

-- process escrow transfers
CREATE OR REPLACE FUNCTION process_escrow_transfer(
  _op_id BIGINT,
  _block_num INT,
  _from_account INT,
  _to_account INT,
  _agent INT,
  _hbd_amount BIGINT,
  _hive_amount BIGINT,
  _escrow_id BIGINT,
  _ratification_deadline TIMESTAMP,
  _escrow_expiration TIMESTAMP
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
  INSERT INTO account_escrow_transfers(
    account, to_account, agent, hbd_amount, hive_amount,
    escrow_id, ratification_deadline, escrow_expiration,
    source_op, source_op_block
  )
  VALUES (
    _from_account, _to_account, _agent, _hbd_amount, _hive_amount,
    _escrow_id, _ratification_deadline, _escrow_expiration,
    _op_id, _block_num
  );
END;
$$;

-- process conversions
CREATE OR REPLACE FUNCTION process_conversion(
  _op_id BIGINT,
  _block_num INT,
  _account INT,
  _amount BIGINT,
  _conversion_date TIMESTAMP
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
  INSERT INTO account_pending_conversions(
    account, amount, conversion_date, conversion_id,
    source_op, source_op_block
  )
  VALUES (
    _account, _amount, _conversion_date, _op_id,
    _op_id, _block_num
  );
END;
$$;

-- remove completed operations
CREATE OR REPLACE FUNCTION remove_completed_operations(
  _block_num INT,
  _current_time TIMESTAMP
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
  -- remove completed orders
  DELETE FROM account_open_orders
  WHERE source_op_block < _block_num - 86400; -- orders older than 24 hours

  -- remove completed escrows
  DELETE FROM account_escrow_transfers
  WHERE escrow_expiration < _current_time;

  -- remove completed conversions
  DELETE FROM account_pending_conversions
  WHERE conversion_date < _current_time;

  -- mark completed savings withdrawals
  UPDATE transfer_saving_id
  SET complete_date = _current_time
  WHERE complete_date IS NULL 
  AND source_op_block < _block_num - 259200; -- 3 days for savings withdrawals
END;
$$;

-- get total balances for an account including new balances
CREATE OR REPLACE FUNCTION get_total_account_balances(
  _account INT
)
RETURNS TABLE (
  total_hive BIGINT,
  total_hbd BIGINT,
  total_vests BIGINT,
  open_orders_hive BIGINT,
  open_orders_hbd BIGINT,
  escrow_hive BIGINT,
  escrow_hbd BIGINT,
  pending_conversions BIGINT,
  savings_withdrawals_hive BIGINT,
  savings_withdrawals_hbd BIGINT
)
LANGUAGE plpgsql
AS
$$
BEGIN
  RETURN QUERY
  WITH base_balances AS (
    SELECT nai, balance 
    FROM current_account_balances 
    WHERE account = _account
  ),
  orders AS (
    SELECT nai, SUM(amount) as amount
    FROM account_open_orders
    WHERE account = _account
    GROUP BY nai
  ),
  escrows AS (
    SELECT 
      SUM(hive_amount) as hive_amount,
      SUM(hbd_amount) as hbd_amount
    FROM account_escrow_transfers
    WHERE account = _account
  ),
  conversions AS (
    SELECT SUM(amount) as amount
    FROM account_pending_conversions
    WHERE account = _account
  ),
  savings AS (
    SELECT nai, SUM(balance) as amount
    FROM transfer_saving_id
    WHERE account = _account
    AND complete_date IS NULL
    GROUP BY nai
  )
  SELECT
    COALESCE((SELECT balance FROM base_balances WHERE nai = 13), 0) +
    COALESCE((SELECT amount FROM orders WHERE nai = 13), 0) +
    COALESCE((SELECT hive_amount FROM escrows), 0) as total_hive,
    
    COALESCE((SELECT balance FROM base_balances WHERE nai = 37), 0) +
    COALESCE((SELECT amount FROM orders WHERE nai = 37), 0) +
    COALESCE((SELECT hbd_amount FROM escrows), 0) +
    COALESCE((SELECT amount FROM conversions), 0) as total_hbd,
    
    COALESCE((SELECT balance FROM base_balances WHERE nai = 50), 0) as total_vests,
    
    COALESCE((SELECT amount FROM orders WHERE nai = 13), 0) as open_orders_hive,
    COALESCE((SELECT amount FROM orders WHERE nai = 37), 0) as open_orders_hbd,
    COALESCE((SELECT hive_amount FROM escrows), 0) as escrow_hive,
    COALESCE((SELECT hbd_amount FROM escrows), 0) as escrow_hbd,
    COALESCE((SELECT amount FROM conversions), 0) as pending_conversions,
    COALESCE((SELECT amount FROM savings WHERE nai = 13), 0) as savings_withdrawals_hive,
    COALESCE((SELECT amount FROM savings WHERE nai = 37), 0) as savings_withdrawals_hbd;
END;
$$;

RESET ROLE;