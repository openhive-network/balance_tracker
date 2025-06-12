SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_orders_delta(
    IN _from_block INT,
    IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  __added INT;
  __closed INT;
BEGIN
  ------------------------------------------------------------------
  -- pull slice once
  ------------------------------------------------------------------
  WITH raw_ops AS (
         SELECT ov.body::jsonb AS body,
                ot.name        AS op_name
           FROM hive.operations_view ov
           JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
          WHERE ov.block_num BETWEEN _from_block AND _to_block
            AND ot.name IN (
                  'hive::protocol::limit_order_create_operation',
                  'hive::protocol::fill_order_operation',
                  'hive::protocol::limit_order_cancelled_operation')
       ),

       ----------------------------------------------------------------
       -- adds = creates that remain open inside this slice
       ----------------------------------------------------------------
       adds AS (
         SELECT (body->'value'->>'owner')::TEXT            AS account_name,
                (body->'value'->>'orderid')::BIGINT        AS order_id,
                (body->'value'->'amount_to_sell'->>'nai')  AS nai,
                (body->'value'->'amount_to_sell'->>'amount')::NUMERIC
                  / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT) AS amount
           FROM raw_ops
          WHERE op_name='hive::protocol::limit_order_create_operation'
            AND body->'value'->>'orderid' IS NOT NULL
            AND NOT EXISTS (         -- same order filled or cancelled inside slice
                  SELECT 1
                    FROM raw_ops r2
                   WHERE r2.op_name IN (
                           'hive::protocol::fill_order_operation',
                           'hive::protocol::limit_order_cancelled_operation')
                     AND (
                          (r2.body->'value'->>'open_orderid')    = (body->'value'->>'orderid') OR
                          (r2.body->'value'->>'current_orderid') = (body->'value'->>'orderid') OR
                          (r2.body->'value'->>'orderid')         = (body->'value'->>'orderid')
                         )
               )
       ),

       ----------------------------------------------------------------
       -- closes = fills or cancels that close *earlier* orders
       ----------------------------------------------------------------
       closes AS (
         SELECT  -- fills: both sides
                COALESCE(
                  (body->'value'->>'open_orderid'),
                  (body->'value'->>'current_orderid'),
                  (body->'value'->>'orderid'))::BIGINT          AS order_id,
                COALESCE(
                  (body->'value'->>'open_owner'),
                  (body->'value'->>'current_owner'),
                  (body->'value'->>'seller'))::TEXT             AS account_name,
                COALESCE(
                  (body->'value'->'open_pays'    ->>'nai'),
                  (body->'value'->'current_pays' ->>'nai'),
                  '@@000000013')                                AS nai,        -- dummy
                0::NUMERIC                                      AS amount      -- we only need counts
           FROM raw_ops
          WHERE op_name IN (
                  'hive::protocol::fill_order_operation',
                  'hive::protocol::limit_order_cancelled_operation')
       ),

       ----------------------------------------------------------------
       -- aggregate   (adds = +1 / closes = −1)
       ----------------------------------------------------------------
       delta AS (
         SELECT account_name,
                SUM(CASE WHEN nai='@@000000013' THEN 1 ELSE 0 END)  AS hbd_cnt_delta,
                SUM(CASE WHEN nai='@@000000013' THEN amount END)    AS hbd_amt_delta,
                SUM(CASE WHEN nai='@@000000021' THEN 1 ELSE 0 END)  AS hive_cnt_delta,
                SUM(CASE WHEN nai='@@000000021' THEN amount END)    AS hive_amt_delta
           FROM (
             SELECT * FROM adds
             UNION ALL
             SELECT * FROM closes   -- closes have amount=0, count should subtract 1
           ) u
          GROUP BY account_name
       ),

       ----------------------------------------------------------------
       -- apply delta: upsert then arithmetic update
       ----------------------------------------------------------------
       upsert AS (
         INSERT INTO btracker_app.account_open_orders_summary AS dst (
                account_name,
                open_orders_hbd_count,
                open_orders_hbd_amount,
                open_orders_hive_count,
                open_orders_hive_amount)
         SELECT account_name,
                0,0,0,0  -- dummy placeholders, will be updated in next step
           FROM delta
         ON CONFLICT (account_name) DO NOTHING
         RETURNING 1
       ),

       apply AS (
         UPDATE btracker_app.account_open_orders_summary dst
            SET open_orders_hbd_count   = GREATEST(0, dst.open_orders_hbd_count   + d.hbd_cnt_delta),
                open_orders_hbd_amount  = GREATEST(0, dst.open_orders_hbd_amount  + d.hbd_amt_delta),
                open_orders_hive_count  = GREATEST(0, dst.open_orders_hive_count  + d.hive_cnt_delta),
                open_orders_hive_amount = GREATEST(0, dst.open_orders_hive_amount + d.hive_amt_delta)
           FROM delta d
          WHERE d.account_name = dst.account_name
         RETURNING 1
       )

  SELECT
      (SELECT COUNT(*) FROM upsert)  INTO __upserted,
      (SELECT COUNT(*) FROM apply)   INTO __closed;   -- rows touched

  RAISE NOTICE
      'Blocks %–%: delta applied to % accounts (new rows %, affected rows %)',
      _from_block, _to_block, __upserted + __closed, __upserted, __closed;
END;
$$;
