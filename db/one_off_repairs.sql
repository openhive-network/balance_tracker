SET ROLE btracker_owner;

/**
 * One-off repairs of already synced state.
 * ========================================
 * A fix to block processing only corrects the rows it rewrites from then on;
 * rows an earlier version wrote stay wrong until an operation touches them
 * again. A repair recomputes those rows once per install: install_app.sh runs
 * run_one_off_repairs() on every install and upgrade, which applies each repair
 * not yet recorded in one_off_repairs and records it.
 *
 * On a fresh install the tables are empty, so each repair changes nothing and
 * is recorded as applied; the fixed processing writes every row from then on.
 *
 * one_off_repairs is not registered with the HAF context: a fork rollback must
 * not make a repair run again.
 *
 * To add a repair: write a function returning the number of rows it changed,
 * and add (name, function) to the list in run_one_off_repairs(). A repair that
 * edits a registered table must not issue the UPDATE or DELETE when it has
 * nothing to change: HAF's statement-level triggers reject any table edit on a forking
 * context that has not processed a block yet (a fresh install).
 */
CREATE TABLE IF NOT EXISTS one_off_repairs
(
  name          TEXT        NOT NULL,
  rows_repaired INT         NOT NULL,
  applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT pk_one_off_repairs PRIMARY KEY (name)
);

/**
 * repair_vesting_withdraw_rate()
 * ------------------------------
 * Recomputes account_withdraws.vesting_withdraw_rate of every active power-down
 * from the withdraw_vesting operation that started it (account_withdraws.source_op),
 * with the rounding process_withdraw_vesting_operation applies (minimum of 1,
 * rounded up from HF21 on). Earlier versions always rounded down.
 *
 * @returns number of power-downs whose rate changed
 */
CREATE OR REPLACE FUNCTION repair_vesting_withdraw_rate()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_withdraw_vesting              INT := btracker_backend.op_withdraw_vesting();
  _hf_vests_precision               INT := btracker_backend.hf_vests_precision();
  _hf_withdraw_rate                 INT := btracker_backend.hf_withdraw_rate();
  _hf_withdraw_rate_round_up        INT := btracker_backend.hf_withdraw_rate_round_up();
  _hf_vests_precision_block         INT;
  _hf_withdraw_rate_block           INT;
  _hf_withdraw_rate_round_up_block  INT;
  __accounts                        INT[];
  __rates                           BIGINT[];
  __repaired                        INT;
BEGIN
  SELECT
    MAX(CASE WHEN hardfork_num = _hf_vests_precision THEN block_num END),
    MAX(CASE WHEN hardfork_num = _hf_withdraw_rate THEN block_num END),
    MAX(CASE WHEN hardfork_num = _hf_withdraw_rate_round_up THEN block_num END)
  INTO _hf_vests_precision_block, _hf_withdraw_rate_block, _hf_withdraw_rate_round_up_block
  FROM hafd.applied_hardforks
  WHERE hardfork_num IN (_hf_vests_precision, _hf_withdraw_rate, _hf_withdraw_rate_round_up);

  SELECT array_agg(r.account), array_agg(r.vesting_withdraw_rate)
  INTO __accounts, __rates
  FROM (
    SELECT
      aw.account,
      aw.vesting_withdraw_rate AS stored_rate,
      (btracker_backend.process_withdraw_vesting_operation(
        ov.body_value,
        ov.block_num > _hf_vests_precision_block,
        ov.block_num > _hf_withdraw_rate_block,
        ov.block_num > _hf_withdraw_rate_round_up_block
      )).vesting_withdraw_rate
    FROM account_withdraws aw
    -- OFFSET 0 keeps the lookup per power-down (a primary-key probe each) instead of
    -- a hash join over every withdraw_vesting operation of the chain.
    CROSS JOIN LATERAL (
      SELECT o.body_value, o.block_num
      FROM operations_view o
      WHERE o.id = aw.source_op AND o.op_type_id = _op_withdraw_vesting
      OFFSET 0
    ) ov
    WHERE aw.to_withdraw > 0
  ) r
  WHERE r.vesting_withdraw_rate IS DISTINCT FROM r.stored_rate;

  IF __accounts IS NULL THEN
    RETURN 0;
  END IF;

  UPDATE account_withdraws aw
  SET vesting_withdraw_rate = u.rate
  FROM unnest(__accounts, __rates) AS u(account, rate)
  WHERE aw.account = u.account;

  GET DIAGNOSTICS __repaired = ROW_COUNT;
  RETURN __repaired;
END
$$;

/**
 * repair_power_downs_without_vests()
 * ----------------------------------
 * Ends the power-down of every account whose VESTS balance is 0 (vesting_withdraw_rate,
 * to_withdraw and withdrawn reset to 0): hived ends a power-down at the payout that
 * leaves the account with 0 VESTS, which earlier versions kept active. A power-down
 * cannot start without VESTS, and only a payout takes an account's last VESTS, so
 * every such power-down is one hived ended.
 *
 * Runs before repair_rc_delegation_overflow, which reads the power-down state.
 *
 * @returns number of power-downs ended
 */
CREATE OR REPLACE FUNCTION repair_power_downs_without_vests()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _nai_vests  INT := btracker_backend.nai_vests();
  __accounts  INT[];
  __repaired  INT;
BEGIN
  SELECT array_agg(aw.account)
  INTO __accounts
  FROM account_withdraws aw
  LEFT JOIN current_account_balances cab ON cab.account = aw.account AND cab.nai = _nai_vests
  WHERE (aw.vesting_withdraw_rate <> 0 OR aw.to_withdraw <> 0 OR aw.withdrawn <> 0)
    AND COALESCE(cab.balance, 0) = 0;

  IF __accounts IS NULL THEN
    RETURN 0;
  END IF;

  UPDATE account_withdraws aw
  SET vesting_withdraw_rate = 0, to_withdraw = 0, withdrawn = 0
  WHERE aw.account = ANY(__accounts);

  GET DIAGNOSTICS __repaired = ROW_COUNT;
  RETURN __repaired;
END
$$;

/**
 * repair_rc_delegation_overflow()
 * -------------------------------
 * Applies hived's implicit RC delegation reduction (remove_rc_delegation_overflow,
 * db/process_rc_delegations.sql) to every delegator whose stored RC delegations
 * exceed its effective vests now:
 *   VESTS balance - delegated vests + received vests - next power-down withdrawal
 * Earlier versions never reduced them. A reduced delegation keeps its source_op.
 *
 * It corrects only the delegations still over their delegator's vesting: one hived
 * reduced at an earlier vest change, after which the delegator's vesting grew back,
 * stays as stored, since the vesting at that change is not kept. Those need a
 * resync to be corrected.
 *
 * Runs after repair_vesting_withdraw_rate and repair_power_downs_without_vests,
 * whose power-down state it reads.
 *
 * @returns number of RC delegations reduced or removed
 */
CREATE OR REPLACE FUNCTION repair_rc_delegation_overflow()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _nai_vests          INT := btracker_backend.nai_vests();
  __delegators        INT[];
  __effective_vests   BIGINT[];
BEGIN
  SELECT array_agg(d.delegator), array_agg(
    COALESCE(cab.balance, 0) - COALESCE(ad.delegated_vests, 0) + COALESCE(ad.received_vests, 0)
    - CASE WHEN aw.to_withdraw > aw.withdrawn THEN LEAST(aw.vesting_withdraw_rate, aw.to_withdraw - aw.withdrawn) ELSE 0 END)
  INTO __delegators, __effective_vests
  FROM (SELECT DISTINCT crd.delegator FROM current_rc_delegations crd) d
  LEFT JOIN current_account_balances cab ON cab.account = d.delegator AND cab.nai = _nai_vests
  LEFT JOIN account_delegations ad ON ad.account = d.delegator
  LEFT JOIN account_withdraws aw ON aw.account = d.delegator;

  IF __delegators IS NULL THEN
    RETURN 0;
  END IF;

  RETURN remove_rc_delegation_overflow(__delegators, __effective_vests, array_fill(NULL::BIGINT, ARRAY[cardinality(__delegators)]));
END
$$;

/**
 * repair_hf_23_withdraw_requests()
 * --------------------------------
 * Deletes the pending savings withdrawals hived removed at HF23 and earlier
 * versions of process_hf_23 left in transfer_saving_id (see
 * delete_hf_23_withdraw_requests). Does nothing until the application has
 * processed the HF23 block: process_hf_23 deletes them then.
 *
 * @returns number of transfer_saving_id rows deleted
 */
CREATE OR REPLACE FUNCTION repair_hf_23_withdraw_requests()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __hardfork_23_block INT := (SELECT block_num FROM hafd.applied_hardforks WHERE hardfork_num = 23);
BEGIN
  IF __hardfork_23_block IS NULL
     OR COALESCE(hive.app_get_current_block_num(current_schema()), 0) < __hardfork_23_block THEN
    RETURN 0;
  END IF;

  RETURN delete_hf_23_withdraw_requests(__hardfork_23_block);
END
$$;

/**
 * delete_hf_23_withdraw_requests(__hardfork_23_block)
 * ---------------------------------------------------
 * Deletes the transfer_saving_id rows of the accounts cleared at HF23 whose
 * transfer_from_savings was requested up to the HF23 block: hived removed those
 * withdrawals without a fill or cancel. Rows of requests made after HF23 are kept.
 * Only valid once the application has processed the HF23 block.
 *
 * @param __hardfork_23_block  block of HF23
 * @returns number of transfer_saving_id rows deleted
 */
CREATE OR REPLACE FUNCTION delete_hf_23_withdraw_requests(__hardfork_23_block INT)
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_transfer_from_savings  INT := btracker_backend.op_transfer_from_savings();
  __accounts                 INT[];
  __request_ids              BIGINT[];
  __repaired                 INT;
BEGIN
  SELECT array_agg(tsi.account), array_agg(tsi.request_id)
  INTO __accounts, __request_ids
  FROM btracker_backend.hf_23_cleared_accounts(__hardfork_23_block) c
  JOIN transfer_saving_id tsi ON tsi.account = c.account_id
  JOIN accounts_view av ON av.id = tsi.account
  -- The account's latest request with this id: hived frees a removed request's
  -- id, so a request made after HF23 may reuse it.
  CROSS JOIN LATERAL (
    SELECT ov.block_num
    -- OFFSET 0 keeps both lookups per account (its transfer_from_savings by
    -- account and type, then a primary-key probe each) instead of a scan of
    -- every operation of the chain for the body filters.
    FROM (
      SELECT aov.operation_id
      FROM account_operations_view aov
      WHERE aov.account_id = tsi.account AND aov.op_type_id = _op_transfer_from_savings
      OFFSET 0
    ) aov
    CROSS JOIN LATERAL (
      SELECT o.block_num, o.body_value
      FROM operations_view o
      WHERE o.id = aov.operation_id
      OFFSET 0
    ) ov
    WHERE ov.body_value->>'from' = av.name
      AND (ov.body_value->>'request_id')::BIGINT = tsi.request_id
    ORDER BY aov.operation_id DESC
    LIMIT 1
  ) req
  WHERE req.block_num <= __hardfork_23_block;

  IF __accounts IS NULL THEN
    RETURN 0;
  END IF;

  DELETE FROM transfer_saving_id tsi
  USING unnest(__accounts, __request_ids) AS s(account, request_id)
  WHERE tsi.account = s.account AND tsi.request_id = s.request_id;

  GET DIAGNOSTICS __repaired = ROW_COUNT;
  RETURN __repaired;
END
$$;

/**
 * backfill_delegation_and_withdraw_history()
 * ------------------------------------------
 * Fills account_delegation_history and account_withdraw_history of an install
 * synced before block processing wrote them, from the application's first block
 * to its current one, with the rows processing would have written: one pass over
 * the operations of each kind (operations_view by type), with the state before the
 * first block empty.
 *
 *   - delegations: process_block_range_delegations' per-operation changes of
 *     delegated and received vests, summed per account;
 *   - power-downs: process_block_range_withdrawals' state after every withdraw_vesting,
 *     HF23 reset and fill. Whether a payout left the account without VESTS is read
 *     from account_balance_history (the account's VESTS after the payout's last
 *     fill), through its block index: the install must have finalized its massive
 *     sync, which builds that index.
 *
 * tests/maintenance/delegation_withdraw_history.sql checks the rows against the ones
 * a sync writes.
 *
 * Like every repair, its rows are recorded by HAF as changes of the context's current
 * block: should a fork revert that block before it becomes irreversible, they are
 * removed with it, and the history before the fork point stays empty until the
 * repair is run again (delete its one_off_repairs row, empty both tables, reinstall).
 *
 * @returns number of history rows written
 */
CREATE OR REPLACE FUNCTION backfill_delegation_and_withdraw_history()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _op_delegate_vesting_shares        INT := btracker_backend.op_delegate_vesting_shares();
  _op_account_create_with_delegation INT := btracker_backend.op_account_create_with_delegation();
  _op_return_vesting_delegation      INT := btracker_backend.op_return_vesting_delegation();
  _op_hardfork_hive                  INT := btracker_backend.op_hardfork_hive();
  _op_withdraw_vesting               INT := btracker_backend.op_withdraw_vesting();
  _op_fill_vesting_withdraw          INT := btracker_backend.op_fill_vesting_withdraw();
  _nai_vests                         INT := btracker_backend.nai_vests();
  _hf_vests_precision_block          INT;
  _hf_withdraw_rate_block            INT;
  _hf_withdraw_rate_round_up_block   INT;
  __to                               INT := COALESCE(hive.app_get_current_block_num(current_schema()), 0);
  __delegation_rows                  INT;
  __withdraw_rows                    INT;
BEGIN
  -- A fresh install (nothing processed yet) has nothing to fill, and HAF rejects
  -- an edit of a registered table of a forking context before its first block.
  IF __to = 0 THEN
    RETURN 0;
  END IF;

  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'btracker: the delegation and power-down history backfill needs a finalized install (account_balance_history''s block index); this one is still in its massive sync at block %. Finish the sync with the previous version first, or sync anew.', __to;
  END IF;

  SELECT
    MAX(CASE WHEN hardfork_num = btracker_backend.hf_vests_precision() THEN block_num END),
    MAX(CASE WHEN hardfork_num = btracker_backend.hf_withdraw_rate() THEN block_num END),
    MAX(CASE WHEN hardfork_num = btracker_backend.hf_withdraw_rate_round_up() THEN block_num END)
  INTO _hf_vests_precision_block, _hf_withdraw_rate_block, _hf_withdraw_rate_round_up_block
  FROM hafd.applied_hardforks;

  RAISE NOTICE 'btracker: backfilling account_delegation_history up to block %...', __to;
  WITH ops AS MATERIALIZED (
    SELECT ov.body_value AS body, ov.op_type_id, ov.id
    FROM operations_view ov
    WHERE
      ov.op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation, _op_return_vesting_delegation, _op_hardfork_hive) AND
      ov.block_num <= __to
  ),
  parsed AS MATERIALIZED (
    SELECT result.delegator, result.delegatee, result.amount AS balance, o.id AS source_op, o.op_type_id
    FROM ops o
    CROSS JOIN LATERAL (
      SELECT (
        CASE
          WHEN o.op_type_id = _op_delegate_vesting_shares
            THEN btracker_backend.process_delegate_vesting_shares_operation(o.body)
          WHEN o.op_type_id = _op_account_create_with_delegation
            THEN btracker_backend.process_account_create_with_delegation_operation(o.body)
          WHEN o.op_type_id = _op_return_vesting_delegation
            THEN btracker_backend.process_return_vesting_shares_operation(o.body)
          ELSE btracker_backend.process_hf23_acccounts(o.body)
        END
      ).*
    ) AS result
  ),
  account_ids AS MATERIALIZED (
    SELECT av.name AS account_name, av.id AS account_id
    FROM accounts_view av
    WHERE av.name IN (SELECT delegator FROM parsed UNION SELECT delegatee FROM parsed)
  ),
  delegation_ops AS MATERIALIZED (
    SELECT dr.account_id AS delegator, de.account_id AS delegatee, p.balance, p.source_op, p.op_type_id
    FROM parsed p
    JOIN account_ids dr ON dr.account_name = p.delegator
    LEFT JOIN account_ids de ON de.account_name = p.delegatee
  ),
  -- HF23 cleared every delegation the account had made before it.
  hf23_resets AS (
    SELECT d.delegator, d.delegatee, 0::BIGINT AS balance, MAX(h.source_op) AS source_op, TRUE AS is_hf23_reset
    FROM delegation_ops d
    JOIN delegation_ops h ON h.delegator = d.delegator AND h.op_type_id = _op_hardfork_hive AND d.source_op < h.source_op
    WHERE d.op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation)
    GROUP BY d.delegator, d.delegatee
  ),
  pair_changes AS (
    SELECT delegator, delegatee, balance, source_op, FALSE AS is_hf23_reset
    FROM delegation_ops
    WHERE op_type_id IN (_op_delegate_vesting_shares, _op_account_create_with_delegation)

    UNION ALL

    SELECT delegator, delegatee, balance, source_op, is_hf23_reset
    FROM hf23_resets
  ),
  delegation_delta AS MATERIALIZED (
    SELECT
      delegator,
      delegatee,
      balance - LAG(balance, 1, 0::BIGINT) OVER w_pair_asc AS balance_delta,
      source_op,
      is_hf23_reset
    FROM pair_changes
    WINDOW w_pair_asc AS (PARTITION BY delegator, delegatee ORDER BY source_op)
  ),
  -- As in process_block_range_delegations: the delegator counts increases only (a
  -- decrease comes back with return_vesting_delegation), except at HF23.
  account_changes AS (
    SELECT delegator AS account_id, source_op, 0::BIGINT AS received_vests,
      CASE WHEN is_hf23_reset THEN balance_delta ELSE GREATEST(balance_delta, 0) END AS delegated_vests
    FROM delegation_delta

    UNION ALL

    SELECT delegatee, source_op, balance_delta, 0
    FROM delegation_delta

    UNION ALL

    SELECT delegator, source_op, 0, balance
    FROM delegation_ops
    WHERE op_type_id = _op_return_vesting_delegation
  ),
  delegation_changes AS (
    SELECT account_id, source_op, SUM(received_vests) AS received_vests, SUM(delegated_vests) AS delegated_vests
    FROM account_changes
    GROUP BY account_id, source_op
    HAVING SUM(received_vests) <> 0 OR SUM(delegated_vests) <> 0
  )
  INSERT INTO account_delegation_history (account, delegated_vests, received_vests, source_op)
  SELECT
    account_id,
    SUM(delegated_vests) OVER w_account_asc,
    SUM(received_vests) OVER w_account_asc,
    source_op
  FROM delegation_changes
  WINDOW w_account_asc AS (PARTITION BY account_id ORDER BY source_op);

  GET DIAGNOSTICS __delegation_rows = ROW_COUNT;

  RAISE NOTICE 'btracker: backfilling account_withdraw_history up to block %...', __to;
  WITH ops AS MATERIALIZED (
    SELECT ov.body_value AS body, ov.op_type_id, ov.id, ov.block_num
    FROM operations_view ov
    WHERE
      ov.op_type_id IN (_op_withdraw_vesting, _op_hardfork_hive, _op_fill_vesting_withdraw) AND
      ov.block_num <= __to
  ),
  parsed AS MATERIALIZED (
    SELECT result.account_name, o.id AS source_op, o.block_num, TRUE AS is_set,
      result.vesting_withdraw_rate, result.to_withdraw, result.withdrawn, NULL::BIGINT AS fill_amount
    FROM ops o
    CROSS JOIN LATERAL (
      SELECT (
        CASE
          WHEN o.op_type_id = _op_withdraw_vesting
            THEN btracker_backend.process_withdraw_vesting_operation(
              o.body,
              o.block_num > _hf_vests_precision_block,
              o.block_num > _hf_withdraw_rate_block,
              o.block_num > _hf_withdraw_rate_round_up_block
            )
          ELSE btracker_backend.process_reset_withdraw_hf23(o.body)
        END
      ).*
    ) AS result
    WHERE o.op_type_id IN (_op_withdraw_vesting, _op_hardfork_hive)

    UNION ALL

    SELECT result.account_name, o.id, o.block_num, FALSE, NULL, NULL, NULL, result.withdrawn
    FROM ops o
    CROSS JOIN btracker_backend.process_fill_vesting_withdraw_operation_for_withdrawals(o.body, o.block_num > _hf_vests_precision_block) AS result
    WHERE o.op_type_id = _op_fill_vesting_withdraw
  ),
  account_ids AS MATERIALIZED (
    SELECT av.name AS account_name, av.id AS account_id
    FROM accounts_view av
    WHERE av.name IN (SELECT DISTINCT account_name FROM parsed)
  ),
  power_down_ops AS MATERIALIZED (
    SELECT ai.account_id, p.source_op, p.block_num, p.is_set, p.vesting_withdraw_rate, p.to_withdraw, p.withdrawn, p.fill_amount
    FROM parsed p
    JOIN account_ids ai ON ai.account_name = p.account_name
  ),
  -- A payout is all fills of an account in one block; hived checks the account's
  -- VESTS once, after the last of them.
  payouts AS (
    SELECT account_id, block_num, MAX(source_op) AS source_op
    FROM power_down_ops
    WHERE NOT is_set
    GROUP BY account_id, block_num
  ),
  payouts_leaving_no_vests AS MATERIALIZED (
    SELECT p.account_id, p.source_op
    FROM payouts p
    LEFT JOIN LATERAL (
      SELECT abh.balance
      FROM account_balance_history abh
      WHERE abh.account = p.account_id AND abh.nai = _nai_vests
        AND hafd.operation_id_to_block_num(abh.source_op) <= p.block_num
        AND abh.source_op <= p.source_op
      ORDER BY hafd.operation_id_to_block_num(abh.source_op) DESC, abh.balance_seq_no DESC
      LIMIT 1
    ) vests ON TRUE
    WHERE COALESCE(vests.balance, 0) = 0
  ),
  timeline_segments AS (
    SELECT
      pdo.*,
      pn.account_id IS NOT NULL AS ends,
      COUNT(*) FILTER (WHERE pdo.is_set) OVER w_account_asc AS segment
    FROM power_down_ops pdo
    LEFT JOIN payouts_leaving_no_vests pn ON pn.account_id = pdo.account_id AND pn.source_op = pdo.source_op
    WINDOW w_account_asc AS (PARTITION BY pdo.account_id ORDER BY pdo.source_op)
  ),
  segment_progress AS (
    SELECT
      ts.account_id,
      ts.source_op,
      ts.is_set,
      ts.vesting_withdraw_rate,
      ts.to_withdraw,
      ts.withdrawn,
      ts.segment,
      FIRST_VALUE(ts.vesting_withdraw_rate) OVER w_segment_asc AS segment_rate,
      FIRST_VALUE(ts.to_withdraw) OVER w_segment_asc AS segment_to_withdraw,
      FIRST_VALUE(ts.withdrawn) OVER w_segment_asc + COALESCE(SUM(ts.fill_amount) OVER w_segment_asc, 0) AS segment_withdrawn,
      ts.ends
    FROM timeline_segments ts
    WHERE ts.segment > 0
    WINDOW w_segment_asc AS (PARTITION BY ts.account_id, ts.segment ORDER BY ts.source_op)
  ),
  segment_end AS (
    SELECT
      sp.*,
      BOOL_OR(NOT sp.is_set AND (sp.segment_withdrawn >= sp.segment_to_withdraw OR sp.ends)) OVER w_segment_asc AS ended
    FROM segment_progress sp
    WINDOW w_segment_asc AS (PARTITION BY sp.account_id, sp.segment ORDER BY sp.source_op)
  )
  INSERT INTO account_withdraw_history (account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op)
  SELECT
    se.account_id,
    CASE WHEN se.is_set THEN se.vesting_withdraw_rate WHEN se.ended THEN 0 ELSE se.segment_rate END,
    CASE WHEN se.is_set THEN se.to_withdraw WHEN se.ended THEN 0 ELSE se.segment_to_withdraw END,
    CASE WHEN se.is_set THEN se.withdrawn WHEN se.ended THEN 0 ELSE se.segment_withdrawn END,
    se.source_op
  FROM segment_end se;

  GET DIAGNOSTICS __withdraw_rows = ROW_COUNT;

  ANALYZE account_delegation_history;
  ANALYZE account_withdraw_history;

  RETURN __delegation_rows + __withdraw_rows;
END
$$;

/**
 * run_one_off_repairs()
 * ---------------------
 * Applies, in list order, every repair not yet recorded in one_off_repairs, each
 * recorded in the same transaction as its changes.
 */
CREATE OR REPLACE FUNCTION run_one_off_repairs()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __repair    RECORD;
  __repaired  INT;
BEGIN
  FOR __repair IN
    SELECT r.name, r.function_name
    FROM (VALUES
      (1, 'vesting_withdraw_rate_hf21_rounding', 'repair_vesting_withdraw_rate'),
      (2, 'power_downs_without_vests', 'repair_power_downs_without_vests'),
      (3, 'rc_delegation_overflow', 'repair_rc_delegation_overflow'),
      (4, 'hf_23_cleared_withdraw_requests', 'repair_hf_23_withdraw_requests'),
      (5, 'delegation_and_withdraw_history', 'backfill_delegation_and_withdraw_history')
    ) AS r(position, name, function_name)
    WHERE NOT EXISTS (SELECT 1 FROM one_off_repairs a WHERE a.name = r.name)
    ORDER BY r.position
  LOOP
    EXECUTE format('SELECT %I()', __repair.function_name) INTO __repaired;
    INSERT INTO one_off_repairs (name, rows_repaired) VALUES (__repair.name, __repaired);
    RAISE NOTICE 'One-off repair % applied: % rows changed', __repair.name, __repaired;
  END LOOP;
END
$$;

RESET ROLE;
