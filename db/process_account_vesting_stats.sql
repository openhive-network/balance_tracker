SET ROLE btracker_owner;

/**
 * Per-account vesting outputs:
 *   1. account_vesting_history             (one row per impacted account per op,
 *                                           with monotonic vesting_seq_no per account)
 *   2. account_vesting_by_day        (per-account daily aggregation)
 *   3. account_vesting_by_month      (per-account monthly aggregation)
 *
 * Yearly granularity is rolled up on the fly from
 * account_vesting_by_month at query time (mirror of how
 * vesting_stats_by_year derives from the global vesting_stats_by_month).
 *
 * Global vesting stats (vesting_stats_by_day/_month) are populated by a
 * separate function (process_vesting_stats) — kept apart for single
 * responsibility and to follow the existing convention
 * (process_balances/process_savings own per-account state, while
 * process_transfer_stats owns global stats).
 *
 * Tracked operations and impacted accounts:
 *   - transfer_to_vesting        -> kind=1 (power_up); from + to are impacted
 *   - withdraw_vesting           -> kind=2 (power_down_init); account only;
 *                                   cancellations (vesting_shares.amount=0) excluded
 *   - fill_vesting_withdraw      -> kind=3 (power_down_fill);
 *                                   from_account + to_account are impacted
 *
 * For each (impacted_account, op) pair, ONE history row + ONE contribution
 * to that account's daily and monthly aggregates is written.
 */
CREATE OR REPLACE FUNCTION process_account_vesting_stats(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET enable_bitmapscan = OFF
AS
$$
DECLARE
  _op_transfer_to_vesting    INT := btracker_backend.op_transfer_to_vesting();
  _op_withdraw_vesting       INT := btracker_backend.op_withdraw_vesting();
  _op_fill_vesting_withdraw  INT := btracker_backend.op_fill_vesting_withdraw();
  _hf_vests_precision        INT := btracker_backend.hf_vests_precision();
  _hf_vests_precision_block  INT;
  __history_count            INT;
  __by_day_count             INT;
  __by_month_count           INT;
BEGIN
  SELECT MAX(block_num)
  INTO _hf_vests_precision_block
  FROM hafd.applied_hardforks
  WHERE hardfork_num = _hf_vests_precision;

  WITH ops AS MATERIALIZED (
    SELECT
      ov.id          AS source_op,
      ov.body_value  AS body,
      ov.op_type_id,
      ov.block_num
    FROM _btracker_ops_batch ov
    WHERE
      ov.op_type_id IN (_op_transfer_to_vesting, _op_withdraw_vesting, _op_fill_vesting_withdraw) AND
      ov.block_num BETWEEN _from AND _to
  ),
  parsed AS MATERIALIZED (
    -- One row per op (cancellations filtered out).
    SELECT
      o.source_op,
      o.block_num,
      o.body,
      (CASE o.op_type_id
        WHEN _op_transfer_to_vesting    THEN 1
        WHEN _op_withdraw_vesting       THEN 2
        WHEN _op_fill_vesting_withdraw  THEN 3
      END)::SMALLINT AS kind,
      (CASE
        WHEN o.op_type_id = _op_transfer_to_vesting
          THEN ((o.body)->'amount'->>'amount')::BIGINT
        WHEN o.op_type_id = _op_fill_vesting_withdraw
         AND ((o.body)->'deposited'->>'precision')::INT = 3
          THEN ((o.body)->'deposited'->>'amount')::BIGINT
        ELSE 0::BIGINT
      END) AS hive_amount,
      (CASE
        WHEN o.op_type_id = _op_withdraw_vesting
          THEN ((o.body)->'vesting_shares'->>'amount')::NUMERIC
               * btracker_backend.vests_precision_multiplier(o.block_num > _hf_vests_precision_block)
        WHEN o.op_type_id = _op_fill_vesting_withdraw
          THEN ((o.body)->'withdrawn'->>'amount')::NUMERIC
               * btracker_backend.vests_precision_multiplier(o.block_num > _hf_vests_precision_block)
        ELSE 0::NUMERIC
      END) AS vests_amount
    FROM ops o
    WHERE NOT (
      o.op_type_id = _op_withdraw_vesting
      AND ((o.body)->'vesting_shares'->>'amount')::BIGINT = 0
    )
  ),
  /* ====================================================================
   * Fan-out: one row per (impacted_account, op).
   * - kind=1 (transfer_to_vesting):   from + to
   * - kind=2 (withdraw_vesting):      account only
   * - kind=3 (fill_vesting_withdraw): from_account + to_account
   * Deduplicated when from = to.
   * ====================================================================
   */
  impacted_names AS MATERIALIZED (
    SELECT p.source_op, p.block_num, p.kind, p.hive_amount, p.vests_amount, acc.name
    FROM parsed p
    CROSS JOIN LATERAL (
      SELECT name FROM (
        VALUES
          (CASE p.kind
            WHEN 1 THEN p.body->>'from'
            WHEN 2 THEN p.body->>'account'
            WHEN 3 THEN p.body->>'from_account'
          END),
          (CASE p.kind
            WHEN 1 THEN p.body->>'to'
            WHEN 3 THEN p.body->>'to_account'
          END)
      ) AS v(name)
      WHERE name IS NOT NULL
    ) acc
  ),
  unique_impacted AS (
    SELECT DISTINCT source_op, block_num, kind, hive_amount, vests_amount, name
    FROM impacted_names
  ),
  account_id_lookup AS MATERIALIZED (
    SELECT av.name, av.id
    FROM hive.accounts_view av
    WHERE av.name IN (SELECT DISTINCT name FROM unique_impacted)
  ),
  impacted_with_ids AS MATERIALIZED (
    SELECT
      ai.id AS account,
      ui.kind,
      ui.hive_amount,
      ui.vests_amount,
      ui.source_op,
      ui.block_num
    FROM unique_impacted ui
    JOIN account_id_lookup ai ON ai.name = ui.name
  ),
  /* ====================================================================
   * (1) account_vesting_history — append rows with TWO seq_nos:
   *   - vesting_seq_no = MAX(per account)        + ROW_NUMBER over source_op  (filter='all')
   *   - kind_seq_no    = MAX(per account, kind)  + ROW_NUMBER over source_op
   *                                                within (account, kind)     (filter=specific)
   * Both are monotonic; either one supports windowed BETWEEN-based pagination
   * (no OFFSET on deep pages), mirroring balance_history's balance_seq_no.
   * ====================================================================
   */
  existing_max_per_account AS (
    SELECT avh.account, MAX(avh.vesting_seq_no) AS max_seq
    FROM account_vesting_history avh
    WHERE avh.account IN (SELECT DISTINCT account FROM impacted_with_ids)
    GROUP BY avh.account
  ),
  existing_max_per_kind AS (
    SELECT avh.account, avh.kind, MAX(avh.kind_seq_no) AS max_seq
    FROM account_vesting_history avh
    WHERE avh.account IN (SELECT DISTINCT account FROM impacted_with_ids)
    GROUP BY avh.account, avh.kind
  ),
  new_history AS (
    SELECT
      iwi.account,
      COALESCE(emp.max_seq, 0)
        + ROW_NUMBER() OVER (PARTITION BY iwi.account ORDER BY iwi.source_op) AS vesting_seq_no,
      COALESCE(emk.max_seq, 0)
        + ROW_NUMBER() OVER (PARTITION BY iwi.account, iwi.kind ORDER BY iwi.source_op) AS kind_seq_no,
      iwi.kind,
      iwi.hive_amount,
      iwi.vests_amount,
      iwi.source_op
    FROM impacted_with_ids iwi
    LEFT JOIN existing_max_per_account emp ON emp.account = iwi.account
    LEFT JOIN existing_max_per_kind    emk ON emk.account = iwi.account AND emk.kind = iwi.kind
  ),
  insert_history AS (
    INSERT INTO account_vesting_history (
      account, vesting_seq_no, kind_seq_no, kind, hive_amount, vests_amount, source_op
    )
    SELECT account, vesting_seq_no, kind_seq_no, kind, hive_amount, vests_amount, source_op
    FROM new_history
    RETURNING 1
  ),
  /* ====================================================================
   * (2) + (3) account_vesting_by_day / _by_month — per-account
   * aggregation via GROUPING SETS over (account, by_day) / (account, by_month).
   * Source: impacted_with_ids (one row per impacted-account/op pair).
   * ====================================================================
   */
  join_blocks_date AS MATERIALIZED (
    SELECT
      iwi.account,
      iwi.kind,
      iwi.hive_amount,
      iwi.vests_amount,
      iwi.block_num,
      date_trunc('day',   bv.created_at) AS by_day,
      date_trunc('month', bv.created_at) AS by_month
    FROM impacted_with_ids iwi
    JOIN hive.blocks_view bv ON bv.num = iwi.block_num
  ),
  account_aggregated AS MATERIALIZED (
    SELECT
      account,
      by_day,
      by_month,
      GROUPING(by_day, by_month) AS grp_level,
      COUNT(*) FILTER (WHERE kind = 1)::INT                                     AS power_up_count,
      COALESCE(SUM(hive_amount)  FILTER (WHERE kind = 1), 0)::BIGINT            AS power_up_hive,
      COUNT(*) FILTER (WHERE kind = 2)::INT                                     AS power_down_init_count,
      COALESCE(SUM(vests_amount) FILTER (WHERE kind = 2), 0)::NUMERIC           AS power_down_init_vests,
      COUNT(*) FILTER (WHERE kind = 3)::INT                                     AS power_down_fill_count,
      COALESCE(SUM(vests_amount) FILTER (WHERE kind = 3), 0)::NUMERIC           AS power_down_fill_vests,
      COALESCE(SUM(hive_amount)  FILTER (WHERE kind = 3), 0)::BIGINT            AS power_down_fill_hive,
      MAX(block_num)::INT                                                        AS last_block_num
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (account, by_day),
      (account, by_month)
    )
  ),
  insert_account_by_day AS (
    INSERT INTO account_vesting_by_day AS avs (
      account, updated_at,
      power_up_count, power_up_hive,
      power_down_init_count, power_down_init_vests,
      power_down_fill_count, power_down_fill_vests, power_down_fill_hive,
      last_block_num
    )
    SELECT
      agg.account, agg.by_day,
      agg.power_up_count, agg.power_up_hive,
      agg.power_down_init_count, agg.power_down_init_vests,
      agg.power_down_fill_count, agg.power_down_fill_vests, agg.power_down_fill_hive,
      agg.last_block_num
    FROM account_aggregated agg
    WHERE agg.grp_level = 1
    ON CONFLICT ON CONSTRAINT pk_account_vesting_by_day DO UPDATE SET
      power_up_count        = avs.power_up_count        + EXCLUDED.power_up_count,
      power_up_hive         = avs.power_up_hive         + EXCLUDED.power_up_hive,
      power_down_init_count = avs.power_down_init_count + EXCLUDED.power_down_init_count,
      power_down_init_vests = avs.power_down_init_vests + EXCLUDED.power_down_init_vests,
      power_down_fill_count = avs.power_down_fill_count + EXCLUDED.power_down_fill_count,
      power_down_fill_vests = avs.power_down_fill_vests + EXCLUDED.power_down_fill_vests,
      power_down_fill_hive  = avs.power_down_fill_hive  + EXCLUDED.power_down_fill_hive,
      last_block_num        = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  ),
  insert_account_by_month AS (
    INSERT INTO account_vesting_by_month AS avs (
      account, updated_at,
      power_up_count, power_up_hive,
      power_down_init_count, power_down_init_vests,
      power_down_fill_count, power_down_fill_vests, power_down_fill_hive,
      last_block_num
    )
    SELECT
      agg.account, agg.by_month,
      agg.power_up_count, agg.power_up_hive,
      agg.power_down_init_count, agg.power_down_init_vests,
      agg.power_down_fill_count, agg.power_down_fill_vests, agg.power_down_fill_hive,
      agg.last_block_num
    FROM account_aggregated agg
    WHERE agg.grp_level = 2
    ON CONFLICT ON CONSTRAINT pk_account_vesting_by_month DO UPDATE SET
      power_up_count        = avs.power_up_count        + EXCLUDED.power_up_count,
      power_up_hive         = avs.power_up_hive         + EXCLUDED.power_up_hive,
      power_down_init_count = avs.power_down_init_count + EXCLUDED.power_down_init_count,
      power_down_init_vests = avs.power_down_init_vests + EXCLUDED.power_down_init_vests,
      power_down_fill_count = avs.power_down_fill_count + EXCLUDED.power_down_fill_count,
      power_down_fill_vests = avs.power_down_fill_vests + EXCLUDED.power_down_fill_vests,
      power_down_fill_hive  = avs.power_down_fill_hive  + EXCLUDED.power_down_fill_hive,
      last_block_num        = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  )
  SELECT
    (SELECT count(*) FROM insert_history)            AS history,
    (SELECT count(*) FROM insert_account_by_day)     AS by_day,
    (SELECT count(*) FROM insert_account_by_month)   AS by_month
  INTO __history_count, __by_day_count, __by_month_count;
END
$$;

RESET ROLE;
