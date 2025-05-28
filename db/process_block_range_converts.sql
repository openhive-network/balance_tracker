SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION process_block_range_converts(
    IN _from INT,
    IN _to   INT
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS $$
DECLARE
  __insert_converts INT;
BEGIN
  WITH
    filled AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')        AS account_name,
        (ov.body::jsonb->'value'->>'requestid')::BIGINT AS request_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name = 'hive::protocol::fill_convert_request_operation'
        AND ov.block_num BETWEEN _from AND _to
    ),
    pending AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')        AS account_name,
        (ov.body::jsonb->'value'->'amount'->>'nai') AS nai,
        (
          (ov.body::jsonb->'value'->'amount'->>'amount')::NUMERIC
          / power(
              10::NUMERIC,
              (ov.body::jsonb->'value'->'amount'->>'precision')::INT
            )
        ) AS total_amount
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name = 'hive::protocol::convert_operation'
        AND (ov.body::jsonb->'value'->>'requestid')::BIGINT NOT IN (SELECT request_id FROM filled)
        AND ov.block_num BETWEEN _from AND _to
    ),
    conv_summary AS (
      SELECT
        p.account_name,
        asset_map.asset,
        COUNT(p.total_amount) AS request_count,
        COALESCE(SUM(p.total_amount), 0) AS total_amount
      FROM (VALUES
        ('@@000000013','HBD'),
        ('@@000000021','HIVE')
      ) AS asset_map(nai, asset)
      LEFT JOIN pending p ON p.nai = asset_map.nai
      GROUP BY p.account_name, asset_map.asset
    ),
    insert_account_pending_converts AS (
      INSERT INTO btracker_app.account_pending_converts
        (account_name, asset, request_count, total_amount)
      SELECT
        account_name, asset, request_count, total_amount
      FROM conv_summary
      ON CONFLICT (account_name, asset) DO UPDATE
        SET request_count = EXCLUDED.request_count,
            total_amount   = EXCLUDED.total_amount
      RETURNING (xmax = 0) AS is_new_entry, account_name
    )
  SELECT count(*) INTO __insert_converts
    FROM insert_account_pending_converts;
END
$$;
