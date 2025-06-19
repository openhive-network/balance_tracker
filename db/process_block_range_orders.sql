SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_orders (
  IN _from_block INT,
  IN _to_block   INT
) RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  c_new_creates      INT;
  c_new_created_ids  INT;
  c_closed_cancels   INT;
  c_new_cancel_ids   INT;
  c_closed_fills     INT;
  c_new_fill_ids     INT;
  c_summary_upserts  INT;
  c_purged           INT;
BEGIN
/*------------------------------------------------------------------
 a) raw operations in this block range
------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_raw_ops;
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
  SELECT ov.body::jsonb AS body,
         ot.name        AS op_name
    FROM hive.operations_view ov
    JOIN hafd.operation_types ot ON ot.id = ov.op_type_id
   WHERE ov.block_num BETWEEN _from_block AND _to_block
     AND ot.name IN (
           'hive::protocol::limit_order_create_operation',
           'hive::protocol::fill_order_operation',
           'hive::protocol::limit_order_cancel_operation',
           'hive::protocol::limit_order_cancelled_operation'
         );

/*------------------------------------------------------------------
 b) INSERT creates — skip orders already known filled
------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_ins_created;
  CREATE TEMP TABLE tmp_ins_created ON COMMIT DROP AS
  WITH already_filled AS (
        SELECT DISTINCT order_id
          FROM btracker_app.order_event_log
         WHERE event_type = 'fill')
  , ins AS (
    INSERT INTO btracker_app.open_orders_detail
                (account_name, order_id, nai, amount)
    SELECT (body->'value'->>'owner')::TEXT,
           (body->'value'->>'orderid')::BIGINT,
           (body->'value'->'amount_to_sell'->>'nai'),
           ((body->'value'->'amount_to_sell'->>'amount')::NUMERIC
              / POWER(10,(body->'value'->'amount_to_sell'->>'precision')::INT))
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::limit_order_create_operation'
      AND (body->'value'->>'orderid') IS NOT NULL
      AND (body->'value'->>'orderid')::BIGINT NOT IN (SELECT order_id FROM already_filled)
    ON CONFLICT DO NOTHING
    RETURNING account_name, order_id
  )
  SELECT * FROM ins;
  SELECT COUNT(*) INTO c_new_creates FROM tmp_ins_created;

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT account_name, order_id, 'create', _to_block
    FROM tmp_ins_created;

/*------------------------------------------------------------------
 c) Apply pending fills that now have a matching order row
------------------------------------------------------------------*/
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

/*------------------------------------------------------------------
 d) Cancel operations
------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_cancels;
  CREATE TEMP TABLE tmp_cancels ON COMMIT DROP AS
  SELECT (body->'value'->>'orderid')::BIGINT                                 AS id,
         COALESCE(body->'value'->>'seller',body->'value'->>'owner')::TEXT    AS acct
    FROM tmp_raw_ops
   WHERE op_name IN ('hive::protocol::limit_order_cancel_operation',
                     'hive::protocol::limit_order_cancelled_operation')
     AND (body->'value'->>'orderid') IS NOT NULL;

  DELETE FROM btracker_app.open_orders_detail d
        USING tmp_cancels c
   WHERE d.account_name = c.acct
     AND d.order_id     = c.id;
  GET DIAGNOSTICS c_closed_cancels = ROW_COUNT;

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'cancel', _to_block FROM tmp_cancels;

/*------------------------------------------------------------------
 e) Fill operations for this range
------------------------------------------------------------------*/
  DROP TABLE IF EXISTS tmp_fill_ops;
  CREATE TEMP TABLE tmp_fill_ops ON COMMIT DROP AS
  SELECT
      -- maker
      (body->'value'->>'open_owner')        ::TEXT   AS acct_m,
      (body->'value'->>'open_orderid')      ::BIGINT AS id_m,
      (body->'value'->'open_pays'->>'nai')           AS nai_m,
      ((body->'value'->'open_pays'->>'amount')::NUMERIC
          / POWER(10,(body->'value'->'open_pays'->>'precision')::INT)) AS amt_m,
      -- taker
      (body->'value'->>'current_owner')     ::TEXT   AS acct_t,
      (body->'value'->>'current_orderid')   ::BIGINT AS id_t,
      (body->'value'->'current_pays'->>'nai')        AS nai_t,
      ((body->'value'->'current_pays'->>'amount')::NUMERIC
          / POWER(10,(body->'value'->'current_pays'->>'precision')::INT)) AS amt_t
  FROM tmp_raw_ops
  WHERE op_name = 'hive::protocol::fill_order_operation';

  /* maker side immediate update */
  UPDATE btracker_app.open_orders_detail d
     SET amount = TRUNC(d.amount - f.amt_m, 3)
    FROM tmp_fill_ops f
   WHERE d.account_name = f.acct_m
     AND d.order_id     = f.id_m
     AND d.nai          = f.nai_m;

  /* taker side immediate update */
  UPDATE btracker_app.open_orders_detail d
     SET amount = TRUNC(d.amount - f.amt_t, 3)
    FROM tmp_fill_ops f
   WHERE d.account_name = f.acct_t
     AND d.order_id     = f.id_t
     AND d.nai          = f.nai_t;

/*------------------------------------------------------------------
 f) Buffer fills whose order row still doesn’t exist
------------------------------------------------------------------*/
  WITH orphan_fills AS (
      SELECT acct_m AS account_name, id_m AS order_id, nai_m AS nai, amt_m AS delta
        FROM tmp_fill_ops f
       WHERE NOT EXISTS (
             SELECT 1 FROM btracker_app.open_orders_detail d
              WHERE d.account_name = f.acct_m
                AND d.order_id     = f.id_m
                AND d.nai          = f.nai_m )
      UNION ALL
      SELECT acct_t, id_t, nai_t, amt_t
        FROM tmp_fill_ops f
       WHERE NOT EXISTS (
             SELECT 1 FROM btracker_app.open_orders_detail d
              WHERE d.account_name = f.acct_t
                AND d.order_id     = f.id_t
                AND d.nai          = f.nai_t )
  ), summed AS (
      SELECT account_name, order_id, nai,
             SUM(delta) AS delta_amount
        FROM orphan_fills
       GROUP BY account_name, order_id, nai
  )
  INSERT INTO btracker_app.pending_fills (account_name, order_id, nai, delta_amount)
  SELECT account_name, order_id, nai, delta_amount
    FROM summed
  ON CONFLICT (account_name, order_id, nai)
  DO UPDATE
        SET delta_amount = btracker_app.pending_fills.delta_amount
                         + EXCLUDED.delta_amount;

/* tmp_fills for logging */
  DROP TABLE IF EXISTS tmp_fills;
  CREATE TEMP TABLE tmp_fills ON COMMIT DROP AS
  SELECT acct_m AS acct, id_m AS id FROM tmp_fill_ops
  UNION ALL
  SELECT acct_t, id_t FROM tmp_fill_ops;

  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;  -- rows affected by the two UPDATEs

  INSERT INTO btracker_app.order_event_log (acct, order_id, event_type, block_num)
  SELECT acct, id, 'fill', _to_block FROM tmp_fills;

/*------------------------------------------------------------------
 g) Purge rows only if **strictly below** dust
------------------------------------------------------------------*/
  DELETE FROM btracker_app.open_orders_detail
   WHERE amount < 0.001;
  GET DIAGNOSTICS c_purged = ROW_COUNT;

/*------------------------------------------------------------------
 h) Re-build per-account summary (unchanged)
------------------------------------------------------------------*/
  WITH snap AS (
    SELECT account_name,
           COUNT(*) FILTER (WHERE nai='@@000000013')               AS open_hbd_cnt,
           COALESCE(SUM(amount) FILTER (WHERE nai='@@000000013'),0) AS open_hbd_amt,
           COUNT(*) FILTER (WHERE nai='@@000000021')               AS open_hive_cnt,
           COALESCE(SUM(amount) FILTER (WHERE nai='@@000000021'),0) AS open_hive_amt
      FROM btracker_app.open_orders_detail
     GROUP BY account_name)
  , up AS (
    INSERT INTO btracker_app.account_open_orders_summary
                (account_name, open_orders_hbd_count, open_orders_hbd_amount,
                 open_orders_hive_count, open_orders_hive_amount)
    SELECT * FROM snap
    ON CONFLICT (account_name) DO UPDATE
          SET open_orders_hbd_count  = EXCLUDED.open_orders_hbd_count,
              open_orders_hbd_amount = EXCLUDED.open_orders_hbd_amount,
              open_orders_hive_count = EXCLUDED.open_orders_hive_count,
              open_orders_hive_amount= EXCLUDED.open_orders_hive_amount
    RETURNING 1)
  SELECT COUNT(*) INTO c_summary_upserts FROM up;

/*---------------------------------------------------------------
   i) Append debug arrays & purge empty summaries (unchanged)
----------------------------------------------------------------*/
  -- created
  WITH a AS (
    UPDATE btracker_app.account_open_orders_summary s
       SET created_order_ids = s.created_order_ids || ic.order_id
      FROM tmp_ins_created ic
     WHERE s.account_name = ic.account_name
       AND NOT ic.order_id = ANY(s.created_order_ids)
    RETURNING 1)
  SELECT COUNT(*) INTO c_new_created_ids FROM a;

  -- cancelled
  WITH a AS (
    UPDATE btracker_app.account_open_orders_summary s
       SET cancelled_order_ids = s.cancelled_order_ids || c.id
      FROM tmp_cancels c
     WHERE s.account_name = c.acct
       AND NOT c.id = ANY(s.cancelled_order_ids)
    RETURNING 1)
  SELECT COUNT(*) INTO c_new_cancel_ids FROM a;

  -- filled
  WITH a AS (
    UPDATE btracker_app.account_open_orders_summary s
       SET filled_order_ids = s.filled_order_ids || f.id
      FROM tmp_fills f
     WHERE s.account_name = f.acct
       AND NOT f.id = ANY(s.filled_order_ids)
    RETURNING 1)
  SELECT COUNT(*) INTO c_new_fill_ids FROM a;

  -- purge empty summaries
  WITH gone AS (
    DELETE FROM btracker_app.account_open_orders_summary
     WHERE open_orders_hbd_count=0
       AND open_orders_hive_count=0
    RETURNING 1)
  SELECT COUNT(*) INTO c_purged FROM gone;

/*------------------------------------------------------------------
 j) Console notice + run-log
------------------------------------------------------------------*/
  RAISE NOTICE
    'Blocks %–% | creates=% (+ids=%) | cancels=% (+ids=%) | fills rows=% (+ids=%) | summary=% | purged=%',
    _from_block, _to_block,
    c_new_creates, c_new_created_ids,
    c_closed_cancels, c_new_cancel_ids,
    c_closed_fills,  c_new_fill_ids,
    c_summary_upserts, c_purged;

  INSERT INTO btracker_app.block_range_run_log
      (from_block, to_block,
       new_creates, cancelled_deleted, fills_deleted,
       summary_upserts, purged,
       created_ids, cancelled_ids, filled_ids)
  VALUES (_from_block, _to_block,
          c_new_creates, c_closed_cancels, c_closed_fills,
          c_summary_upserts, c_purged,
          COALESCE((SELECT array_agg(order_id) FROM tmp_ins_created), '{}'),
          COALESCE((SELECT array_agg(id)        FROM tmp_cancels),   '{}'),
          COALESCE((SELECT array_agg(id)        FROM tmp_fills),     '{}'));
END;
$$;
