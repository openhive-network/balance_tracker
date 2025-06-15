SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_converts(
    IN _from_block INT,
    IN _to_block   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  c_new_requests    INT;
  c_closed_fills    INT;
  c_summary_upserts INT;
  c_purged          INT;
BEGIN
  /* 0) Clean up any leftover temp table */
  DROP TABLE IF EXISTS tmp_raw_ops;

  /* 1) Cache all convert ops in this block range */
  CREATE TEMP TABLE tmp_raw_ops ON COMMIT DROP AS
  SELECT
    ov.body::jsonb AS body,
    ot.name        AS op_name
  FROM hive.operations_view ov
  JOIN hafd.operation_types ot
    ON ot.id = ov.op_type_id
  WHERE ov.block_num BETWEEN _from_block AND _to_block
    AND ot.name IN (
      'hive::protocol::convert_operation',
      'hive::protocol::fill_convert_request_operation'
    );

  /* 2) Insert new convert requests into detail */
  WITH new_reqs AS (
    SELECT
      (body->'value'->>'owner')              AS account_name,
      (body->'value'->>'requestid')::BIGINT  AS request_id,
      (body->'value'->'amount'->>'nai')      AS nai,
      ((body->'value'->'amount'->>'amount')::NUMERIC
         / POWER(
             10::NUMERIC,
             (body->'value'->'amount'->>'precision')::INT
           )
      )                                      AS amount
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::convert_operation'
      AND (body->'value'->>'requestid') IS NOT NULL
  ), ins AS (
    INSERT INTO btracker_app.pending_converts_detail
      (account_name, request_id, nai, amount)
    SELECT account_name, request_id, nai, amount
      FROM new_reqs
    ON CONFLICT DO NOTHING
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_new_requests FROM ins;

  /* 3) Delete any requests that got filled */
  DELETE FROM btracker_app.pending_converts_detail d
  USING (
    SELECT
      (body->'value'->>'requestid')::BIGINT AS request_id,
      (body->'value'->>'owner')             AS account_name
    FROM tmp_raw_ops
    WHERE op_name = 'hive::protocol::fill_convert_request_operation'
      AND (body->'value'->>'requestid') IS NOT NULL
  ) AS f
  WHERE d.account_name = f.account_name
    AND d.request_id   = f.request_id;
  GET DIAGNOSTICS c_closed_fills = ROW_COUNT;

  /* 4) Rebuild & upsert summary from detail */
  WITH snap AS (
    SELECT
      account_name,
      COUNT(*) FILTER (WHERE nai = '@@000000013')            AS hbd_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000013'), 0) AS hbd_amount,
      COUNT(*) FILTER (WHERE nai = '@@000000021')            AS hive_count,
      COALESCE(SUM(amount) FILTER (WHERE nai = '@@000000021'), 0) AS hive_amount
    FROM btracker_app.pending_converts_detail
    GROUP BY account_name
  ),
  up_summary AS (
    -- HBD row
    INSERT INTO btracker_app.account_pending_converts
      (account_name, asset, request_count, total_amount)
    SELECT account_name, 'HBD', hbd_count, hbd_amount FROM snap
    UNION ALL
    -- HIVE row
    SELECT account_name, 'HIVE', hive_count, hive_amount FROM snap
    ON CONFLICT (account_name, asset) DO UPDATE
      SET request_count = EXCLUDED.request_count,
          total_amount  = EXCLUDED.total_amount
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_summary_upserts FROM up_summary;

  /* 5) Purge any summary rows that are zero OR have no matching detail rows */
  WITH purge AS (
    DELETE FROM btracker_app.account_pending_converts apc
    WHERE (apc.request_count = 0 AND apc.total_amount = 0)
      OR NOT EXISTS (
        SELECT 1
        FROM btracker_app.pending_converts_detail d
        WHERE d.account_name = apc.account_name
          AND (
               (apc.asset = 'HBD'  AND d.nai = '@@000000013')
            OR (apc.asset = 'HIVE' AND d.nai = '@@000000021')
          )
      )
    RETURNING 1
  )
  SELECT COUNT(*) INTO c_purged FROM purge;

  /* 6) Final log */
  RAISE NOTICE
    'Blocks %–% | new_requests=% | fills_closed=% | summary_upserts=% | purged=%',
    _from_block, _to_block,
    c_new_requests,
    c_closed_fills,
    c_summary_upserts,
    c_purged;
END;
$$;
