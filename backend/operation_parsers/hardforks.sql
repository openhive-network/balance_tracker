/*
Hardfork-specific processing functions.
Called by: Multiple process files (process_balances.sql, process_delegations.sql, etc.)

Handles HF23 (Hive fork from Steem) balance resets:
- Steemit Inc accounts had all balances zeroed
- Affected: main balances, rewards, savings, delegations, withdrawals

This function processes hardfork_hive_operation at the specific HF23 block,
resetting all financial state for impacted accounts to zero.

Note: This function may become unnecessary after changes to hardfork_hive_operation
in HAF that provide more structured data.
*/

SET ROLE btracker_owner;

-- Process HF23 balance resets for Steemit Inc accounts.
-- At the Hive fork, affected accounts had all financial state zeroed:
-- - Main balances (HIVE, HBD, VESTS)
-- - Pending rewards
-- - Savings balances and pending withdrawals
-- Delegations and withdrawals are handled by their respective process files.
CREATE OR REPLACE FUNCTION btracker_backend.process_hf_23(__hardfork_23_block INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_hardfork_hive INT := btracker_backend.op_hardfork_hive();
  __balances INT;
  __rewards INT;
  __savings INT;
BEGIN
  -- Find all accounts affected by HF23 at the specific block
  WITH hardfork_hive_operation AS MATERIALIZED (
    SELECT
      (SELECT av.id FROM accounts_view av WHERE av.name = ov.body_value->>'account') AS account_id
    FROM operations_view ov
    WHERE ov.op_type_id = _op_hardfork_hive AND block_num = __hardfork_23_block
  ),
  -- Zero out main account balances
  update_balances AS (
    UPDATE current_account_balances cab SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE cab.account = hho.account_id
    RETURNING cab.account AS cleaned_account_id
  ),
  -- Zero out pending rewards
  update_rewards AS (
    UPDATE account_rewards ar SET
      balance = 0
    FROM hardfork_hive_operation hho
    WHERE ar.account = hho.account_id
    RETURNING ar.account AS cleaned_account_id
  ),
  -- Zero out savings and clear pending withdrawal requests
  update_savings AS (
    UPDATE account_savings acs SET
      balance = 0,
      savings_withdraw_requests = 0
    FROM hardfork_hive_operation hho
    WHERE acs.account = hho.account_id
    RETURNING acs.account AS cleaned_account_id
  )
  SELECT
    (SELECT count(*) FROM update_balances) AS balances,
    (SELECT count(*) FROM update_rewards) AS rewards,
    (SELECT count(*) FROM update_savings) AS savings
  INTO __balances, __rewards, __savings;

END
$$;

-- Balance-clearing virtual operations in a batch (#61).
--
-- hived empties two accounts in consensus, every block in which they hold anything:
--   * clear_null_account_balance_operation   -> 'null': liquid, savings, vesting and reward
--     balances are burned (database::clear_null_account_balance);
--   * consolidate_treasury_balance_operation -> 'steem.dao' (the pre-HF24 treasury): the
--     same balances move to hive.fund (database::consolidate_treasury_balance).
-- Neither op names the account it empties (it is a protocol constant), and HAF's balance
-- collector deliberately emits no debit for it, leaving the clear to consumers - the same
-- contract process_hf_23 honours for hardfork_hive_operation. get_impacted_balances stays
-- a faithful port of that collector (tests/parity); the pipelines apply these clears
-- after it: process_balances and process_savings reset the running balance at the op,
-- process_rewards zeroes the account before applying later rewards.
--
-- process_withdrawals resets account_withdraws.delayed_vests at the same ops (hived clears
-- the delayed votes HF24 introduced along with the vesting shares).
--
-- Known limit, not reached on mainnet (checked on a full node 2026-10-23: btracker holds no
-- savings or reward rows for steem.dao):
--   * hive.fund's side of a consolidate stays as HAF's collector emits it: total_moved is
--     credited as liquid, while hived moves savings and pending rewards into hive.fund's
--     savings and rewards.
--
-- Reads the batch staged by btracker_prefetch_operations. Returns one row per op.
CREATE OR REPLACE FUNCTION btracker_backend.account_clears_in_range(_from INT, _to INT)
RETURNS TABLE (
  account_id INT,
  source_op BIGINT,
  op_type_id SMALLINT,
  source_op_block INT
)
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
BEGIN
  -- Never returns a NULL account: callers split rows by EXISTS over account_id. (Both
  -- accounts exist on any chain that emits these ops.)
  RETURN QUERY
  SELECT av.id::INT, ob.id::BIGINT, ob.op_type_id::SMALLINT, ob.block_num::INT
  FROM _btracker_ops_batch ob
  JOIN accounts_view av
    ON av.name = CASE WHEN ob.op_type_id = _op_clear_null THEN 'null' ELSE 'steem.dao' END
  WHERE ob.op_type_id IN (_op_clear_null, _op_consolidate)
    AND ob.block_num BETWEEN _from AND _to;
END
$$;

-- =============================================================================
-- Repair of 'null' / 'steem.dao' data processed before the clears (#61)
-- =============================================================================
-- A node that processed blocks with an older btracker holds, for these two accounts, every
-- credit but none of the clears: an ever-growing liquid/savings history, inflated current
-- balances, balance_totals and rollups, and pending rewards / delayed vests that hived
-- emptied long ago. rebuild_cleared_accounts() recomputes all of it to exactly what this
-- code produces when it processes the same blocks from genesis; repair_cleared_accounts()
-- runs it once per install, where it is safe.

-- The rebuild itself. Bounded by the context's current block; reads the context views.
--   * liquid and savings history: every stored row is turned back into its delta (its
--     balance minus the previous row's; rows a clear wrote are dropped and re-derived), a
--     marker is added at every clearing op, and the series is recomputed with the reset rule
--     of process_balances / process_savings: each clear debits exactly the balance held at
--     the op, later credits are kept. Sequence numbers are renumbered from 1.
--   * current_account_balances / account_savings / balance_totals follow from the series;
--     the day/month rollups are rewritten when they are maintained (indexes created; during
--     massive sync finalize_massive_sync_before_forking rebuilds them from history after).
--   * pending rewards and delayed vests have no history: they are recomputed from the
--     operations after the account's last clear, with the pipelines' own parsers.
--   * account_hbd_interest: last_balance follows the repaired HBD series and the accrued
--     hbd_seconds are dropped (they were accrued on the phantom balance; a clean replay would
--     hold a small accrual there instead of 0). hbd_seconds_last_update and
--     hbd_last_interest_payment keep their values (a replay would hold the last clear's
--     time); with last_balance 0 neither accrues anything. Nothing reads them for these two
--     accounts' balances.
--   * On the finalize path (install upgraded in the middle of massive sync) the history
--     indexes do not exist yet: the reads and deletes of the two accounts' rows scan the
--     history tables, once, inside a finalize that scans them for the rollups anyway.
-- Running it again reproduces its own result.
CREATE OR REPLACE FUNCTION btracker_backend.rebuild_cleared_accounts(_context TEXT)
RETURNS BIGINT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_clear_null      INT := btracker_backend.op_clear_null_account_balance();
  _op_consolidate     INT := btracker_backend.op_consolidate_treasury_balance();
  _op_author_reward   INT := btracker_backend.op_author_reward();
  _op_benefactor      INT := btracker_backend.op_comment_benefactor_reward();
  _op_curation_reward INT := btracker_backend.op_curation_reward();
  _op_fill_withdraw   INT := btracker_backend.op_fill_vesting_withdraw();
  _op_vesting_done    INT := btracker_backend.op_transfer_to_vesting_completed();
  _op_delayed_voting  INT := btracker_backend.op_delayed_voting();
  _hf24_block         INT := (SELECT ah.block_num FROM hafd.applied_hardforks ah
                              WHERE ah.hardfork_num = btracker_backend.hf_delayed_voting());
  _hf25_block         INT := (SELECT ah.block_num FROM hafd.applied_hardforks ah
                              WHERE ah.hardfork_num = btracker_backend.hf_hbd_interest());
  _head               INT := (SELECT c.current_block_num FROM hafd.contexts c WHERE c.name = _context);
  _rollups            BOOLEAN := btracker_backend.history_indexes_exist(_context);
  _rows               BIGINT;
  _acc                RECORD;
  _op                 RECORD;
  _delayed            BIGINT;
BEGIN
  IF COALESCE(_head, 0) = 0 THEN
    RETURN 0;
  END IF;

  DROP TABLE IF EXISTS _rb_clears;
  CREATE TEMP TABLE _rb_clears AS
  SELECT av.id AS account, ov.id AS source_op, ov.op_type_id::SMALLINT AS op_type_id
  FROM operations_view ov
  JOIN accounts_view av
    ON av.name = CASE WHEN ov.op_type_id = _op_clear_null THEN 'null' ELSE 'steem.dao' END
  WHERE ov.op_type_id IN (_op_clear_null, _op_consolidate)
    AND ov.block_num <= _head;

  -- ---------------------------------------------------------------- liquid balances
  DROP TABLE IF EXISTS _rb_balances;
  CREATE TEMP TABLE _rb_balances AS
  WITH hist AS (
    SELECT
      h.account, h.nai, h.op_type_id, h.source_op, h.balance_seq_no AS ord,
      h.balance - LAG(h.balance, 1, 0::BIGINT) OVER (PARTITION BY h.account, h.nai ORDER BY h.balance_seq_no) AS delta
    FROM account_balance_history h
    WHERE h.account IN (SELECT DISTINCT c.account FROM _rb_clears c)
  ),
  u AS (
    SELECT account, nai, op_type_id, source_op, ord, delta, FALSE AS is_clear
    FROM hist WHERE op_type_id NOT IN (_op_clear_null, _op_consolidate)
    UNION ALL
    SELECT c.account, n.nai, c.op_type_id, c.source_op, 0, 0::BIGINT, TRUE
    FROM _rb_clears c CROSS JOIN (VALUES (13::SMALLINT), (21::SMALLINT), (37::SMALLINT)) AS n(nai)
  ),
  g AS (
    SELECT u.*,
      SUM(u.is_clear::INT) OVER (PARTITION BY u.account, u.nai ORDER BY u.source_op, u.ord
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - u.is_clear::INT AS grp
    FROM u
  ),
  d AS (
    SELECT account, nai, op_type_id, source_op, ord, is_clear,
      CASE WHEN is_clear THEN (-SUM(delta) OVER (PARTITION BY account, nai, grp))::BIGINT ELSE delta END AS delta
    FROM g
  )
  SELECT
    account, nai, op_type_id, source_op,
    (SUM(delta) OVER w)::BIGINT AS balance,
    (ROW_NUMBER() OVER w)::INT AS balance_seq_no
  FROM d
  WHERE NOT is_clear OR delta <> 0
  WINDOW w AS (PARTITION BY account, nai ORDER BY source_op, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

  -- balance_totals: replace the two accounts' old contribution by the new one
  WITH old_rows AS (
    SELECT cab.nai, cab.balance FROM current_account_balances cab
    WHERE cab.account IN (SELECT DISTINCT r.account FROM _rb_balances r)
  ),
  new_rows AS (
    SELECT DISTINCT ON (r.account, r.nai) r.nai, r.balance
    FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ),
  diff AS (
    SELECT x.nai, SUM(x.holders) AS holders, SUM(x.total) AS total
    FROM (
      SELECT nai, (balance > 0)::INT AS holders, balance::NUMERIC AS total FROM new_rows
      UNION ALL
      SELECT nai, -(balance > 0)::INT, -balance::NUMERIC FROM old_rows
    ) x
    GROUP BY x.nai
  )
  UPDATE balance_totals bt
  SET holders = bt.holders + diff.holders, total = bt.total + diff.total
  FROM diff WHERE bt.nai = diff.nai AND (diff.holders <> 0 OR diff.total <> 0);

  DELETE FROM account_balance_history h WHERE h.account IN (SELECT DISTINCT r.account FROM _rb_balances r);
  INSERT INTO account_balance_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
  SELECT r.account, r.nai, r.op_type_id, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no;
  GET DIAGNOSTICS _rows = ROW_COUNT;

  INSERT INTO current_account_balances AS cab (account, nai, balance_change_count, source_op, balance)
  SELECT DISTINCT ON (r.account, r.nai) r.account, r.nai, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_balances r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO UPDATE SET
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op;

  IF _rollups THEN
    DELETE FROM balance_history_by_day d WHERE d.account IN (SELECT DISTINCT r.account FROM _rb_balances r);
    DELETE FROM balance_history_by_month m WHERE m.account IN (SELECT DISTINCT r.account FROM _rb_balances r);
    INSERT INTO balance_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('day', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_balances h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
    INSERT INTO balance_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('month', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_balances h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);
  END IF;

  -- HBD interest: last_balance is the repaired liquid HBD after the last change the
  -- accumulator saw (process_balances stops feeding it at HF25); the hbd_seconds accrued on
  -- the phantom balance are dropped.
  UPDATE account_hbd_interest ahi
  SET last_balance = COALESCE((SELECT r.balance FROM _rb_balances r
                               WHERE r.account = ahi.account AND r.nai = 13
                                 AND (_hf25_block IS NULL OR hafd.operation_id_to_block_num(r.source_op) <= _hf25_block)
                               ORDER BY r.balance_seq_no DESC LIMIT 1), 0),
      hbd_seconds = 0
  WHERE ahi.account IN (SELECT DISTINCT c.account FROM _rb_clears c);

  -- ---------------------------------------------------------------- savings
  DROP TABLE IF EXISTS _rb_savings;
  CREATE TEMP TABLE _rb_savings AS
  WITH hist AS (
    SELECT
      h.account, h.nai, h.op_type_id, h.source_op, h.balance_seq_no AS ord,
      h.balance - LAG(h.balance, 1, 0::BIGINT) OVER (PARTITION BY h.account, h.nai ORDER BY h.balance_seq_no) AS delta
    FROM account_savings_history h
    WHERE h.account IN (SELECT DISTINCT c.account FROM _rb_clears c)
  ),
  u AS (
    SELECT account, nai, op_type_id, source_op, ord, delta, FALSE AS is_clear
    FROM hist WHERE op_type_id NOT IN (_op_clear_null, _op_consolidate)
    UNION ALL
    SELECT c.account, n.nai, c.op_type_id, c.source_op, 0, 0::BIGINT, TRUE
    FROM _rb_clears c CROSS JOIN (VALUES (13::SMALLINT), (21::SMALLINT)) AS n(nai)
  ),
  g AS (
    SELECT u.*,
      SUM(u.is_clear::INT) OVER (PARTITION BY u.account, u.nai ORDER BY u.source_op, u.ord
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - u.is_clear::INT AS grp
    FROM u
  ),
  d AS (
    SELECT account, nai, op_type_id, source_op, ord, is_clear,
      CASE WHEN is_clear THEN (-SUM(delta) OVER (PARTITION BY account, nai, grp))::BIGINT ELSE delta END AS delta
    FROM g
  )
  SELECT
    account, nai, op_type_id, source_op,
    (SUM(delta) OVER w)::BIGINT AS balance,
    (ROW_NUMBER() OVER w)::INT AS balance_seq_no
  FROM d
  WHERE NOT is_clear OR delta <> 0
  WINDOW w AS (PARTITION BY account, nai ORDER BY source_op, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

  DELETE FROM account_savings_history h WHERE h.account IN (SELECT DISTINCT r.account FROM _rb_savings r);
  INSERT INTO account_savings_history (account, nai, op_type_id, balance_seq_no, source_op, balance)
  SELECT r.account, r.nai, r.op_type_id, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_savings r ORDER BY r.account, r.nai, r.balance_seq_no;

  -- savings_withdraw_requests is kept: the clears do not cancel pending withdrawals
  INSERT INTO account_savings AS acs (account, nai, balance_change_count, source_op, balance)
  SELECT DISTINCT ON (r.account, r.nai) r.account, r.nai, r.balance_seq_no, r.source_op, r.balance
  FROM _rb_savings r ORDER BY r.account, r.nai, r.balance_seq_no DESC
  ON CONFLICT ON CONSTRAINT pk_account_savings DO UPDATE SET
    balance = EXCLUDED.balance,
    balance_change_count = EXCLUDED.balance_change_count,
    source_op = EXCLUDED.source_op;

  IF _rollups THEN
    DELETE FROM saving_history_by_day d WHERE d.account IN (SELECT DISTINCT r.account FROM _rb_savings r);
    DELETE FROM saving_history_by_month m WHERE m.account IN (SELECT DISTINCT r.account FROM _rb_savings r);
    INSERT INTO saving_history_by_day (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('day', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_savings h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('day', bv.created_at);
    INSERT INTO saving_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT h.account, h.nai,
      (array_agg(h.source_op ORDER BY h.balance_seq_no DESC))[1],
      date_trunc('month', bv.created_at),
      (array_agg(h.balance ORDER BY h.balance_seq_no DESC))[1],
      MIN(h.balance), MAX(h.balance)
    FROM _rb_savings h
    JOIN blocks_view bv ON bv.num = hafd.operation_id_to_block_num(h.source_op)
    GROUP BY h.account, h.nai, date_trunc('month', bv.created_at);
  END IF;

  -- ---------------------------------------------------------------- pending rewards
  -- What the account holds is what was credited after its last clear (neither account can
  -- claim). Same parsers, filters and VESTS->HIVE conversion as process_rewards.
  FOR _acc IN
    SELECT c.account, av.name AS account_name, MAX(c.source_op) AS last_clear
    FROM _rb_clears c JOIN accounts_view av ON av.id = c.account
    GROUP BY c.account, av.name
  LOOP
    -- Only an account with reward rows has anything to recompute (and only then is the
    -- window since its last clear scanned).
    IF EXISTS (SELECT 1 FROM account_rewards ar WHERE ar.account = _acc.account) THEN
    WITH ops AS (
      SELECT ov.id, ov.op_type_id, ov.block_num, ov.body_value AS body
      FROM operations_view ov
      WHERE ov.op_type_id IN (_op_author_reward, _op_benefactor, _op_curation_reward)
        AND ov.block_num BETWEEN hafd.operation_id_to_block_num(_acc.last_clear) AND _head
        AND ov.id > _acc.last_clear
    ),
    credited AS (
      SELECT r.hbd_payout AS hbd, r.hive_payout AS hive, r.vesting_payout AS vests, o.block_num
      FROM ops o
      CROSS JOIN LATERAL (
        SELECT (CASE WHEN o.op_type_id = _op_author_reward
                     THEN btracker_backend.process_author_reward_operation(o.body, o.block_num)
                     ELSE btracker_backend.process_benefactor_reward_operation(o.body, o.block_num) END).*
      ) r
      WHERE o.op_type_id IN (_op_author_reward, _op_benefactor)
        AND (o.body->>'payout_must_be_claimed')::BOOLEAN
        AND r.account_name = _acc.account_name
      UNION ALL
      SELECT 0, 0, r.reward, o.block_num
      FROM ops o
      CROSS JOIN LATERAL btracker_backend.process_curation_rewards(o.body) r
      WHERE o.op_type_id = _op_curation_reward
        AND r.payout_must_be_claimed
        AND r.account_name = _acc.account_name
    ),
    sums AS (
      SELECT
        COALESCE(SUM(c.hbd), 0) AS hbd,
        COALESCE(SUM(c.hive), 0) AS hive,
        COALESCE(SUM(c.vests), 0) AS vests,
        COALESCE(SUM((c.vests * bv.total_vesting_fund_hive::NUMERIC
                      / NULLIF(bv.total_vesting_shares, 0)::NUMERIC)::BIGINT), 0) AS vests_as_hive
      FROM credited c
      LEFT JOIN blocks_view bv ON bv.num = c.block_num
    )
    UPDATE account_rewards ar
    SET balance = CASE ar.nai WHEN 13 THEN s.hbd WHEN 21 THEN s.hive WHEN 37 THEN s.vests
                              WHEN 38 THEN s.vests_as_hive ELSE ar.balance END
    FROM sums s
    WHERE ar.account = _acc.account;
    END IF;

    -- ------------------------------------------------------------ delayed vests (HF24+)
    -- Same clamp as process_withdrawals: what was delayed after the last clear.
    _delayed := 0;
    CONTINUE WHEN NOT EXISTS (SELECT 1 FROM account_withdraws aw WHERE aw.account = _acc.account);
    FOR _op IN
      SELECT r.from_account, r.withdrawn, r.to_account, r.deposited
      FROM operations_view ov
      CROSS JOIN LATERAL (
        SELECT (CASE
                  WHEN ov.op_type_id = _op_fill_withdraw
                    THEN btracker_backend.process_fill_vesting_withdraw_operation(ov.body_value)
                  WHEN ov.op_type_id = _op_vesting_done
                    THEN btracker_backend.process_transfer_to_vesting_completed_operation(ov.body_value)
                  ELSE btracker_backend.process_delayed_voting_operation(ov.body_value) END).*
      ) r
      WHERE ov.op_type_id IN (_op_fill_withdraw, _op_vesting_done, _op_delayed_voting)
        AND ov.block_num BETWEEN hafd.operation_id_to_block_num(_acc.last_clear) AND _head
        AND ov.block_num > COALESCE(_hf24_block, 2147483647)
        AND ov.id > _acc.last_clear
        AND (r.from_account = _acc.account_name OR r.to_account = _acc.account_name)
      ORDER BY ov.id
    LOOP
      IF _op.from_account = _acc.account_name THEN
        _delayed := GREATEST(_delayed + COALESCE(_op.withdrawn, 0), 0);
      END IF;
      IF _op.to_account = _acc.account_name THEN
        _delayed := GREATEST(_delayed + COALESCE(_op.deposited, 0), 0);
      END IF;
    END LOOP;
    UPDATE account_withdraws aw SET delayed_vests = _delayed
    WHERE aw.account = _acc.account AND aw.delayed_vests IS DISTINCT FROM _delayed;
  END LOOP;

  DROP TABLE _rb_clears;
  DROP TABLE _rb_balances;
  DROP TABLE _rb_savings;
  RETURN _rows;
END
$$;

-- Whether this schema's history indexes exist (the LIVE layout). Schema-qualified, unlike
-- isIndexesCreated(), which finds the index of any schema - btracker_app and hafbe_bal on
-- the same database both create it.
CREATE OR REPLACE FUNCTION btracker_backend.history_indexes_exist(_context TEXT)
RETURNS BOOLEAN
LANGUAGE 'sql' STABLE
AS
$$
  SELECT to_regclass(format('%I.idx_account_balance_nai_balance_idx', _context)) IS NOT NULL;
$$;

-- Rebuild and record, unless btracker_repairs already holds the repair (a fresh install
-- records 'not needed' when it creates its tables). Callers make it safe to write:
--   * the repair_cleared_accounts procedure (installer) detaches the context group first;
--   * finalize_massive_sync_before_forking calls it while the context is non-forking.
-- Returns whether it repaired.
CREATE OR REPLACE FUNCTION btracker_backend.apply_cleared_accounts_repair(_context TEXT)
RETURNS BOOLEAN
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _head INT;
  _rows BIGINT;
BEGIN
  IF EXISTS (SELECT 1 FROM btracker_repairs r WHERE r.name = 'cleared_accounts') THEN
    RETURN FALSE;
  END IF;
  _head := (SELECT c.current_block_num FROM hafd.contexts c WHERE c.name = _context);
  _rows := btracker_backend.rebuild_cleared_accounts(_context);
  INSERT INTO btracker_repairs (name, outcome, at_block)
  VALUES ('cleared_accounts', format('repaired: %s liquid history rows', _rows), _head);
  RAISE NOTICE 'btracker: null/steem.dao repaired (#61) at block %, % liquid history rows', _head, _rows;
  RETURN TRUE;
END
$$;

-- The installer's path (called at the very end of install_app.sh, as the install user,
-- under the processor's install lock in the production image). On an install whose history
-- indexes exist: detach the context group (HAF rewinds it to its irreversible block, drops
-- the fork-rewind triggers and serves irreversible views), commit, rebuild, commit, attach
-- again, commit - so nothing goes through the shadow tables, no fork can undo the repair,
-- and the exclusive locks of detach/attach are held for those steps only, not for the
-- rebuild (minutes on mainnet). If it fails after the detach the group stays detached until
-- the processor's next iteration re-attaches it (hive.update_attachment); the repair is
-- then retried by the next install. On an install still in massive sync it does nothing:
-- finalize_massive_sync_before_forking repairs at the end of the sync.
-- _commit => FALSE skips the commits (tests run it inside a transaction they roll back).
DROP FUNCTION IF EXISTS btracker_backend.repair_cleared_accounts(TEXT, BOOLEAN);  -- its pre-review form
CREATE OR REPLACE PROCEDURE btracker_backend.repair_cleared_accounts(_context TEXT, _commit BOOLEAN DEFAULT TRUE)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _group    TEXT[];  -- not hive.contexts_group: that domain rejects the NULL a declaration starts with
  _attached BOOLEAN;
BEGIN
  IF EXISTS (SELECT 1 FROM btracker_repairs r WHERE r.name = 'cleared_accounts') THEN
    RETURN;
  END IF;
  IF NOT btracker_backend.history_indexes_exist(_context) THEN
    RAISE NOTICE 'btracker: null/steem.dao repair (#61) deferred to the end of massive sync';
    RETURN;
  END IF;

  _attached := hive.app_context_is_attached(_context);
  IF _attached THEN
    SELECT a.contexts::TEXT[] INTO _group
    FROM hafd.applications a WHERE a.contexts @> ARRAY[_context::TEXT];
    _group := COALESCE(_group, ARRAY[_context::TEXT]);
    PERFORM hive.app_context_detach(_group::hive.contexts_group);
    IF _commit THEN COMMIT; END IF;
  END IF;

  PERFORM btracker_backend.apply_cleared_accounts_repair(_context);
  IF _commit THEN COMMIT; END IF;

  IF _attached THEN
    PERFORM hive.app_context_attach(_group::hive.contexts_group);
    IF _commit THEN COMMIT; END IF;
  END IF;
END
$$;

RESET ROLE;
