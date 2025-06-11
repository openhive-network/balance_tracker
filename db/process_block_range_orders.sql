SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_orders()
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $$
BEGIN
  WITH open_summary AS (
    SELECT
      lo.owner AS account_name,
      COUNT(*) FILTER (WHERE lo.for_sale->>'nai' = '@@000000013') AS open_orders_hbd_count,
      COUNT(*) FILTER (WHERE lo.for_sale->>'nai' = '@@000000021') AS open_orders_hive_count,
      COALESCE(
        SUM(
          (lo.for_sale->>'amount')::NUMERIC
          / POWER(10::NUMERIC, (lo.for_sale->>'precision')::INT)
        ) FILTER (WHERE lo.for_sale->>'nai' = '@@000000021'),
        0
      ) AS open_orders_hive_amount,
      COALESCE(
        SUM(
          (lo.for_sale->>'amount')::NUMERIC
          / POWER(10::NUMERIC, (lo.for_sale->>'precision')::INT)
        ) FILTER (WHERE lo.for_sale->>'nai' = '@@000000013'),
        0
      ) AS open_orders_hbd_amount
    FROM hive.limit_orders lo
    GROUP BY lo.owner
  )
  INSERT INTO btracker_app.account_open_orders_summary
    (account_name,
     open_orders_hbd_count,
     open_orders_hive_count,
     open_orders_hive_amount,
     open_orders_hbd_amount)
  SELECT
    account_name,
    open_orders_hbd_count,
    open_orders_hive_count,
    open_orders_hive_amount,
    open_orders_hbd_amount
  FROM open_summary
  ON CONFLICT (account_name) DO UPDATE
    SET open_orders_hbd_count   = EXCLUDED.open_orders_hbd_count,
        open_orders_hive_count  = EXCLUDED.open_orders_hive_count,
        open_orders_hive_amount = EXCLUDED.open_orders_hive_amount,
        open_orders_hbd_amount  = EXCLUDED.open_orders_hbd_amount;
END;
$$;
