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
  /* ====================================================================
   * One row per (impacted_account, op). ALL body extraction and the issue #54
   * attribution (routed fills split into the sender's power_down_fill and the
   * recipient's power_down_route_received) live in get_impacted_vesting; this
   * function only fans the op out and aggregates. transfer_to_vesting emits
   * from + to, collapsed to one when from = to by the DISTINCT below.
   * ==================================================================== */
  impacted AS MATERIALIZED (
    SELECT
      o.source_op,
      o.block_num,
      iv.kind,
      iv.hive_amount,
      iv.vests_amount,
      iv.account_name AS name
    FROM ops o
    JOIN hafd.operation_types ot ON ot.id = o.op_type_id
    CROSS JOIN LATERAL btracker_backend.get_impacted_vesting(
      replace(ot.name, 'hive::protocol::', ''),
      o.body,
      o.block_num > _hf_vests_precision_block
    ) iv
  ),
  unique_impacted AS (
    SELECT DISTINCT source_op, block_num, kind, hive_amount, vests_amount, name
    FROM impacted
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
