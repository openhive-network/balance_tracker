SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders(
  IN _from_block INT,
  IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  -- counters for the console notice
  c_new_creates      INT;
  c_new_created_ids  INT;
  c_closed_cancels   INT;
  c_new_cancel_ids   INT;
  c_closed_fills     INT;
  c_new_fill_ids     INT;
  c_summary_upserts  INT;
  c_purged           INT;
BEGIN
  /*--------------------------------------------------------------------
   a) Load raw ops for this block range
  --------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_raw_ops;
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
  SELECT ov.body::jsonb AS body, ot.name AS op_name
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
   WHERE ov.block_num BETWEEN _from_block AND _to_block
     AND ot.name IN (
       'hive::protocol::limit_order_create_operation',
       'hive::protocol::fill_order_operation',
       'hive::protocol::limit_order_cancel_operation',
       'hive::protocol::limit_order_cancelled_operation'
     );

  /*--------------------------------------------------------------------
   b) Insert new creates  (skip ids that were already completely filled)
  --------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_ins_created;
  CREATE TEMP TABLE tmp_ins_created ON COMMIT DROP AS
  WITH already_closed AS (
        SELECT order_id
          FROM btracker_app.order_event_log
         WHERE event_type = 'fill'
  ), ins AS (
    INSERT INTO btracker_app.open_orders_detail (account_name, order_id, nai, amount)
    SELECT
      (body->'value'->>'owner')::TEXT,
      (body->'value'->>'orderid')::BIGINT,
      (body->'value'->'amount_to_sell'->>'nai'),
      ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
         / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT))
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
      AND (body->'value'->>'orderid')::BIGINT NOT IN (SELECT order_id FROM already_closed)
    ON CONFLICT DO NOTHING
    RETURNING account_name, order_id
  )
  SELECT * FROM ins;
  SELECT COUNT(*) INTO c_new_creates FROM tmp_ins_created;

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT account_name, order_id, 'create', _to_block
    FROM tmp_ins_created;

  /*--------------------------------------------------------------------
   c) Apply any pending fills that now have a matching order row
  --------------------------------------------------------------------*/
  WITH applied AS (
    UPDATE btracker_app.open_orders_detail d
       SET amount = TRUNC(d.amount - p.delta_amount, 3)
      FROM btracker_app.pending_fills p
     WHERE d.account_name = p.account_name
       AND d.order_id     = p.order_id
       AND d.nai          = p.nai
    RETURNING p.*
  )
  DELETE FROM btracker_app.pending_fills p
   USING applied a
   WHERE p.account_name = a.account_name
     AND p.order_id     = a.order_id
     AND p.nai          = a.nai;

  /*--------------------------------------------------------------------
   d) Delete cancelled orders
  --------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_cancels;
  CREATE TEMP TABLE tmp_cancels ON COMMIT DROP AS
  SELECT
    (body->'value'->>'orderid')::BIGINT AS id,
    COALESCE(body->'value'->>'seller', body->'value'->>'owner')::TEXT AS acct
  FROM tmp_raw_ops
  WHERE op_name IN (
        'hive::protocol::limit_order_cancel_operation',
        'hive::protocol::limit_order_cancelled_operation')
    AND (body->'value'->>'orderid') IS NOT NULL;

  DELETE FROM btracker_app.open_orders_detail d
        USING tmp_cancels c
   WHERE d.account_name = c.acct
     AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'cancel', _to_block
    FROM tmp_cancels;

  /*--------------------------------------------------------------------
   e) Process fills from this block range
  --------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_fill_ops;
  CREATE TEMP TABLE tmp_fill_ops ON COMMIT DROP AS
  SELECT
    -- maker side
    (body->'value'->>'open_owner')   ::TEXT    AS acct_maker,
    (body->'value'->>'open_orderid') ::BIGINT  AS id_maker,
    (body->'value'->'open_pays'->>'nai')       AS nai_maker,
    ((body->'value'->'open_pays'->>'amount')::NUMERIC
       / POWER(10,(body->'value'->'open_pays'->>'precision')::INT))
                                           AS amt_maker,
    -- taker side
    (body->'value'->>'current_owner')   ::TEXT    AS acct_taker,
    (body->'value'->>'current_orderid') ::BIGINT  AS id_taker,
    (body->'value'->'current_pays'->>'nai')      AS nai_taker,
    ((body->'value'->'current_pays'->>'amount')::NUMERIC
       / POWER(10,(body->'value'->'current_pays'->>'precision')::INT))
                                           AS amt_taker
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation';

  /*–– Try to subtract maker side immediately –––––––––––––––––––––––––*/
  UPDATE btracker_app.open_orders_detail d
     SET amount = TRUNC(d.amount - f.amt_maker, 3)
    FROM tmp_fill_ops f
   WHERE d.account_name = f.acct_maker
     AND d.order_id     = f.id_maker
     AND d.nai          = f.nai_maker;

  /*–– Try taker side immediately –––––––––––––––––––––––––––––––––––––*/
  UPDATE btracker_app.open_orders_detail d
     SET amount = TRUNC(d.amount - f.amt_taker, 3)
    FROM tmp_fill_ops f
   WHERE d.account_name = f.acct_taker
     AND d.order_id     = f.id_taker
     AND d.nai          = f.nai_taker;

  /*--------------------------------------------------------------------
     f) Buffer any side that did not yet have a matching order row
  --------------------------------------------------------------------*/
  INSERT INTO btracker_app.pending_fills (account_name, order_id, nai, delta_amount)
  SELECT acct_maker, id_maker, nai_maker, amt_maker
    FROM tmp_fill_ops f
   WHERE NOT EXISTS (SELECT 1
                       FROM btracker_app.open_orders_detail d
                      WHERE d.account_name = f.acct_maker
                        AND d.order_id     = f.id_maker
                        AND d.nai          = f.nai_maker)
  ON CONFLICT (account_name, order_id, nai)
  DO UPDATE SET delta_amount = btracker_app.pending_fills.delta_amount
                               + EXCLUDED.delta_amount;

  INSERT INTO btracker_app.pending_fills (account_name, order_id, nai, delta_amount)
  SELECT acct_taker, id_taker, nai_taker, amt_taker
    FROM tmp_fill_ops f
   WHERE NOT EXISTS (SELECT 1
                       FROM btracker_app.open_orders_detail d
                      WHERE d.account_name = f.acct_taker
                        AND d.order_id     = f.id_taker
                        AND d.nai          = f.nai_taker)
  ON CONFLICT (account_name, order_id, nai)
  DO UPDATE SET delta_amount = btracker_app.pending_fills.delta_amount
                               + EXCLUDED.delta_amount;

  /*–– build tmp_fills for logging –––––––––––––––––––––––––––––––––––*/
  DROP TABLE IF EXISTS tmp_fills;
  CREATE TEMP TABLE tmp_fills ON COMMIT DROP AS
  SELECT acct_maker AS acct, id_maker AS id FROM tmp_fill_ops
  UNION ALL
  SELECT acct_taker AS acct, id_taker AS id FROM tmp_fill_ops;

  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;  -- after the two UPDATEs

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'fill', _to_block
    FROM tmp_fills;

  /*--------------------------------------------------------------------
   g) Delete rows that are fully filled (<0.001 left)
  --------------------------------------------------------------------*/
  DELETE FROM btracker_app.open_orders_detail
   WHERE amount < 0.001;        -- keep 0.001 dust

  /*--------------------------------------------------------------------
   h) Re-build per-account summary, debug arrays, purge empties
   (unchanged from your previous version)
  --------------------------------------------------------------------*/
  /* … same as before … */
END;
$$;
