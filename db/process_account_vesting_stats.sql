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
 *   - fill_vesting_withdraw      -> from_account -> kind=3 (power_down_fill):
 *                                     vests = withdrawn; hive = deposited HIVE ONLY for an
 *                                     own power-down (to_account = from_account), so HIVE
 *                                     routed away is NOT credited to the sender.
 *                                   fill_vesting_withdraw -> to_account -> kind=4
 *                                     (power_down_route_received), ONLY when routed
 *                                     (to_account <> from_account): the recipient gains the
 *                                     deposited asset — HIVE (auto_vest=false) or VESTS
 *                                     (auto_vest=true). The recipient is NOT powering down,
 *                                     so it is kept apart from power_down_fill (issue #54).
 *
 * Aggregates are stored TALL: one row per (account, kind, period) with generic
 * op_count / hive_amount / vests_amount; the API pivots kinds into its wide shape.
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
      END) AS vests_amount,
      -- fill_vesting_withdraw 'deposited' asset: precision 3 = HIVE, 6 = VESTS (auto_vest route).
      (CASE
        WHEN o.op_type_id = _op_fill_vesting_withdraw
          THEN ((o.body)->'deposited'->>'precision')::INT
        ELSE NULL::INT
      END) AS deposited_precision,
      -- deposited VESTS — only when the recipient is paid in VESTS (auto_vest=true route).
      (CASE
        WHEN o.op_type_id = _op_fill_vesting_withdraw
         AND ((o.body)->'deposited'->>'precision')::INT = 6
          THEN ((o.body)->'deposited'->>'amount')::NUMERIC
               * btracker_backend.vests_precision_multiplier(o.block_num > _hf_vests_precision_block)
        ELSE 0::NUMERIC
      END) AS deposited_vests
    FROM ops o
    WHERE NOT (
      o.op_type_id = _op_withdraw_vesting
      AND ((o.body)->'vesting_shares'->>'amount')::BIGINT = 0
    )
  ),
  /* ====================================================================
   * Fan-out: one row per (impacted_account, op), each carrying the amounts
   * that THAT account actually experienced (per issue #54). The emitted `kind`
   * is the per-row EVENT kind (1/2/3/4), which becomes the history/aggregate kind.
   *   - kind=1 (transfer_to_vesting):   from + to, each carries the power-up HIVE
   *   - kind=2 (withdraw_vesting):      account only, carries power-down-init VESTS
   *   - kind=3 (fill, from_account):    power_down_fill. vests = withdrawn;
   *                                     hive = deposited HIVE only when NOT routed away
   *                                     (to_account = from_account).
   *   - kind=4 (fill, to_account):      power_down_route_received, ONLY when routed
   *                                     (to_account <> from_account): recipient gains the
   *                                     deposited asset (HIVE auto_vest=false / VESTS otherwise).
   * The kind=1 self power-up (from = to) is collapsed by DISTINCT below.
   * ====================================================================
   */
  impacted_names AS MATERIALIZED (
    -- kind 1: transfer_to_vesting -> from + to, each carries the power-up HIVE.
    SELECT p.source_op, p.block_num, 1::SMALLINT AS kind,
           p.hive_amount, 0::NUMERIC AS vests_amount, p.body->>'from' AS name
    FROM parsed p WHERE p.kind = 1
    UNION ALL
    SELECT p.source_op, p.block_num, 1::SMALLINT,
           p.hive_amount, 0::NUMERIC, p.body->>'to'
    FROM parsed p WHERE p.kind = 1

    -- kind 2: withdraw_vesting -> account only, carries the power-down-init VESTS.
    UNION ALL
    SELECT p.source_op, p.block_num, 2::SMALLINT,
           0::BIGINT, p.vests_amount, p.body->>'account'
    FROM parsed p WHERE p.kind = 2

    -- kind 3: fill_vesting_withdraw FROM side -> own power-down (power_down_fill).
    --   vests = withdrawn; hive = deposited HIVE only for an own power-down (to = from).
    UNION ALL
    SELECT p.source_op, p.block_num, 3::SMALLINT,
           (CASE WHEN p.deposited_precision = 3
                  AND (p.body->>'to_account') = (p.body->>'from_account')
                 THEN p.hive_amount ELSE 0::BIGINT END),
           p.vests_amount,
           p.body->>'from_account'
    FROM parsed p WHERE p.kind = 3

    -- kind 4: fill_vesting_withdraw TO side -> power_down_route_received, ONLY when routed.
    --   The recipient gains the deposited asset: HIVE (auto_vest=false) or VESTS (auto_vest=true).
    UNION ALL
    SELECT p.source_op, p.block_num, 4::SMALLINT,
           (CASE WHEN p.deposited_precision = 3 THEN p.hive_amount     ELSE 0::BIGINT  END),
           (CASE WHEN p.deposited_precision = 6 THEN p.deposited_vests ELSE 0::NUMERIC END),
           p.body->>'to_account'
    FROM parsed p
    WHERE p.kind = 3 AND (p.body->>'to_account') <> (p.body->>'from_account')
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
   * (2) + (3) account_vesting_by_day / _by_month — per-account TALL
   * aggregation via GROUPING SETS over (account, kind, by_day) /
   * (account, kind, by_month). One row per (account, kind, period) with
   * generic op_count / hive_amount / vests_amount.
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
      kind,
      by_day,
      by_month,
      GROUPING(by_day, by_month) AS grp_level,
      COUNT(*)::INT                          AS op_count,
      COALESCE(SUM(hive_amount),  0)::BIGINT  AS hive_amount,
      COALESCE(SUM(vests_amount), 0)::NUMERIC AS vests_amount,
      MAX(block_num)::INT                     AS last_block_num
    FROM join_blocks_date
    GROUP BY GROUPING SETS (
      (account, kind, by_day),
      (account, kind, by_month)
    )
  ),
  insert_account_by_day AS (
    INSERT INTO account_vesting_by_day AS avs (
      account, kind, updated_at, op_count, hive_amount, vests_amount, last_block_num
    )
    SELECT
      agg.account, agg.kind, agg.by_day, agg.op_count, agg.hive_amount, agg.vests_amount, agg.last_block_num
    FROM account_aggregated agg
    WHERE agg.grp_level = 1
    ON CONFLICT ON CONSTRAINT pk_account_vesting_by_day DO UPDATE SET
      op_count       = avs.op_count     + EXCLUDED.op_count,
      hive_amount    = avs.hive_amount  + EXCLUDED.hive_amount,
      vests_amount   = avs.vests_amount + EXCLUDED.vests_amount,
      last_block_num = EXCLUDED.last_block_num
    RETURNING (xmax = 0) AS is_new_entry
  ),
  insert_account_by_month AS (
    INSERT INTO account_vesting_by_month AS avs (
      account, kind, updated_at, op_count, hive_amount, vests_amount, last_block_num
    )
    SELECT
      agg.account, agg.kind, agg.by_month, agg.op_count, agg.hive_amount, agg.vests_amount, agg.last_block_num
    FROM account_aggregated agg
    WHERE agg.grp_level = 2
    ON CONFLICT ON CONSTRAINT pk_account_vesting_by_month DO UPDATE SET
      op_count       = avs.op_count     + EXCLUDED.op_count,
      hive_amount    = avs.hive_amount  + EXCLUDED.hive_amount,
      vests_amount   = avs.vests_amount + EXCLUDED.vests_amount,
      last_block_num = EXCLUDED.last_block_num
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
