/*
Repair of 'null' / 'steem.dao' data processed before the balance clears (#61).

A node that processed blocks with an older btracker holds, for these two accounts, every
credit but none of the clears: an ever-growing liquid/savings history, inflated current
balances, balance_totals and rollups, and pending rewards / delayed vests that hived emptied
long ago. rebuild_cleared_accounts() recomputes all of it to exactly what the processing code
produces when it processes the same blocks from genesis; repair_cleared_accounts() (the
installer) and apply_cleared_accounts_repair() (finalize_massive_sync_before_forking) run it
once per install, where it is safe.

Loaded after db/process_*.sql: the rebuild reuses their parsers and the HBD-interest
accumulator (btracker_backend.accumulate_hbd_interest). The clears themselves are applied at
sync time by the processing code (see btracker_backend.account_clears_in_range in
backend/operation_parsers/hardforks.sql).
*/

SET ROLE btracker_owner;

-- forms these functions had in earlier #61 review rounds
DROP FUNCTION IF EXISTS btracker_backend.rebuild_cleared_accounts(TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS btracker_backend.cleared_accounts_source_gap(TEXT);

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

-- First block from which HAF still holds both the block headers and the operations. 1 on
-- a full HAF; higher on a pruned one (hive.prune_blocks_data drops one contiguous prefix of
-- blocks, operations, transactions and applied_hardforks, keeping the applications' own
-- tables). Read from the irreversible tables pruning deletes from, by primary key: the
-- context views would scan every operation while the context is attached.
CREATE OR REPLACE FUNCTION btracker_backend.first_available_block(_context TEXT)
RETURNS INT
LANGUAGE 'sql' STABLE
AS
$$
  SELECT GREATEST(
    (SELECT MIN(b.num) FROM hafd.blocks b),
    (SELECT hafd.operation_id_to_block_num(MIN(o.id)) FROM hafd.operations o));
$$;

-- The first block the rebuild reads: the one before the first history row of the two
-- accounts (the HBD-interest replay reads the previous block's time), at least 1. NULL when
-- they have no history, i.e. nothing to repair.
CREATE OR REPLACE FUNCTION btracker_backend.cleared_accounts_first_needed_block(_context TEXT)
RETURNS INT
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _first_row INT;
BEGIN
  -- The first row of each (account, nai) series is its balance_seq_no 1 (sequence numbers
  -- follow source_op): with the nai bound, one probe of the (account, nai, balance_seq_no)
  -- index per series instead of reading the history. (On the finalize path, before that
  -- index exists, it is one scan of each history table.)
  EXECUTE format(
    'SELECT MIN(hafd.operation_id_to_block_num(x.source_op)) FROM ('
    '  SELECT h.source_op FROM %1$I.account_balance_history h'
    '    WHERE h.balance_seq_no = 1 AND h.nai IN (13, 21, 37)'
    '      AND h.account IN (SELECT av.id FROM %1$I.accounts_view av WHERE av.name IN (''null'', ''steem.dao''))'
    '  UNION ALL'
    '  SELECT h.source_op FROM %1$I.account_savings_history h'
    '    WHERE h.balance_seq_no = 1 AND h.nai IN (13, 21)'
    '      AND h.account IN (SELECT av.id FROM %1$I.accounts_view av WHERE av.name IN (''null'', ''steem.dao''))'
    ') x', _context) INTO _first_row;
  IF _first_row IS NULL THEN
    RETURN NULL;  -- (GREATEST below would turn it into 1: it ignores NULLs)
  END IF;
  RETURN GREATEST(_first_row - 1, 1);
END
$$;

-- Why the rebuild cannot run here, or NULL when it can. It replays the whole history of
-- 'null' / 'steem.dao' and needs, from _first_needed on (default: computed now), every
-- clearing op and every block's timestamp, plus the HF24/HF25 records that bound the
-- delayed-vests and interest replays. A pruned HAF can have dropped any of these; the
-- rebuild would then miss old clears, lose old rollups or misplace the hardforks. Nothing
-- to repair is not a gap.
-- Pruning that advances WHILE the rebuild runs (another application on the same HAF can
-- hold the boundary back, then let it move) is caught by the rebuild calling this again
-- after its last read, with the boundary it started from.
CREATE OR REPLACE FUNCTION btracker_backend.cleared_accounts_source_gap(_context TEXT, _first_needed INT DEFAULT NULL)
RETURNS TEXT
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  _needed          INT := COALESCE(_first_needed, btracker_backend.cleared_accounts_first_needed_block(_context));
  _first_available INT;
BEGIN
  IF _needed IS NULL THEN
    RETURN NULL;
  END IF;
  _first_available := btracker_backend.first_available_block(_context);
  IF _first_available IS NULL OR _first_available > _needed THEN
    RETURN format('HAF holds blocks and operations only from block %s, but the history of null/steem.dao needs them from block %s (pruned HAF)',
                  COALESCE(_first_available::TEXT, 'none'), _needed);
  END IF;
  -- pruning drops applied_hardforks rows too; a full HAF has them from block 1 on
  IF _first_available > 1
     AND (NOT EXISTS (SELECT 1 FROM hafd.applied_hardforks ah WHERE ah.hardfork_num = btracker_backend.hf_delayed_voting())
          OR NOT EXISTS (SELECT 1 FROM hafd.applied_hardforks ah WHERE ah.hardfork_num = btracker_backend.hf_hbd_interest())) THEN
    RETURN format('HAF holds data only from block %s and has no HF24/HF25 record, which the rebuild needs (pruned HAF)', _first_available);
  END IF;
  RETURN NULL;
END
$$;

-- The rebuild itself. Bounded by the context's current block; reads the context views.
--   * liquid and savings history: every stored row is turned back into its delta (its
--     balance minus the previous row's; rows a clear wrote are dropped and re-derived), a
--     marker is added at every clearing op, and the series is recomputed with the reset rule
--     of process_balances / process_savings: each clear debits exactly the balance held at
--     the op, later credits are kept. Sequence numbers are renumbered from 1.
--   * current_account_balances / account_savings / balance_totals follow from the series;
--     the day rollups are rewritten from it and the month rollups from the days, as
--     finalize_massive_sync_before_forking builds them, when they are maintained (indexes
--     created; during massive sync that finalize rebuilds them from history afterwards).
--   * pending rewards and delayed vests have no history: they are recomputed from the
--     operations after the account's last clear, with the pipelines' own parsers.
--   * account_hbd_interest: the repaired HBD rows are staged as process_balances stages a
--     batch's (into the repair's own _rb_hbd_rows), and accumulate_hbd_interest replays
--     them from no state. It reads
--     hive.blocks_view, which holds the same blocks as the context's view up to the
--     context's block when that block is irreversible: what both callers ensure (the
--     installer detaches the context first, which rewinds it to its irreversible block;
--     finalize runs while the context is non-forking, i.e. processes irreversible blocks).
--   * The source must stay complete: it refuses to start when HAF no longer holds every
--     block and operation it reads (cleared_accounts_source_gap - a pruned HAF, where only a
--     resync can rebuild), and checks again after its last read, so that pruning which
--     advanced while it ran fails it instead of being read as missing history. Both raise
--     SQLSTATE BT061, after which the caller's transaction (or subtransaction) must be
--     rolled back: apply_cleared_accounts_repair does that and records nothing.
--   * On the finalize path (install upgraded in the middle of massive sync) the history
--     indexes do not exist yet: the reads and deletes of the two accounts' rows scan the
--     history tables, once, inside a finalize that scans them for the rollups anyway.
--     Known limit there: hive.app_next_iteration has already moved the context's block to
--     the end of the range about to be processed, so the pending-rewards and delayed-vests
--     sums also take in that range, which processing then applies again - a surplus until
--     the account's next clear resets it. The liquid and savings series are not affected
--     on mainnet: hived clears both accounts in every block that leaves them a balance, so
--     the processed history ends at zero and a clear in that range debits nothing (its
--     marker is dropped).
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
  _needed             INT;
  _rows               BIGINT;
  _acc                RECORD;
  _op                 RECORD;
  _delayed            BIGINT;
  _gap                TEXT;
BEGIN
  IF COALESCE(_head, 0) = 0 THEN
    RETURN 0;
  END IF;
  -- before the history is rewritten: the boundary the final check holds the source to
  _needed := btracker_backend.cleared_accounts_first_needed_block(_context);
  _gap := btracker_backend.cleared_accounts_source_gap(_context, _needed);
  IF _gap IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = 'BT061', MESSAGE = format('btracker: cannot rebuild null/steem.dao (#61): %s', _gap);
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
  ANALYZE _rb_balances;  -- the per-row probes below need the planner to see its size

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
    -- the month from its days, as finalize_massive_sync_before_forking derives it
    INSERT INTO balance_history_by_month (account, nai, source_op, updated_at, balance, min_balance, max_balance)
    SELECT d.account, d.nai,
      (array_agg(d.source_op ORDER BY d.updated_at DESC))[1],
      date_trunc('month', d.updated_at),
      (array_agg(d.balance ORDER BY d.updated_at DESC))[1],
      MIN(d.min_balance), MAX(d.max_balance)
    FROM balance_history_by_day d
    WHERE d.account IN (SELECT DISTINCT r.account FROM _rb_balances r)
    GROUP BY d.account, d.nai, date_trunc('month', d.updated_at);
  END IF;

  -- HBD interest: the repaired liquid-HBD rows, staged exactly as process_balances stages a
  -- batch's (clears included, at trx_in_block -1 like the markers it stages; up to HF25,
  -- inclusive), then folded by accumulate_hbd_interest from no previous state (its epoch
  -- defaults). trx_in_block is not kept in the history: it is read from the operation. An
  -- account with nothing staged keeps no row, as a clean replay leaves it.
  -- Its own staging table, not the batch's _hbd_batch_rows: on the finalize path this runs
  -- as btracker_owner (finalize_massive_sync_before_forking is SECURITY DEFINER) in a
  -- session where another role - hafbe's processor - created _hbd_batch_rows, and only a
  -- temp table's owner may drop it.
  PERFORM btracker_backend.create_hbd_batch_rows('_rb_hbd_rows');
  INSERT INTO _rb_hbd_rows (account_id, balance, source_op, source_op_block, balance_seq_no, trx_in_block)
  SELECT r.account, r.balance, r.source_op, hafd.operation_id_to_block_num(r.source_op), r.balance_seq_no,
         CASE WHEN r.op_type_id IN (_op_clear_null, _op_consolidate) THEN (-1)::SMALLINT
              ELSE ov.trx_in_block::SMALLINT END
  FROM _rb_balances r
  JOIN operations_view ov ON ov.id = r.source_op
  WHERE r.nai = 13
    AND (_hf25_block IS NULL OR hafd.operation_id_to_block_num(r.source_op) <= _hf25_block);
  DELETE FROM account_hbd_interest ahi WHERE ahi.account IN (SELECT DISTINCT r.account FROM _rb_balances r WHERE r.nai = 13);
  PERFORM btracker_backend.accumulate_hbd_interest(1, _head, btracker_backend.hbd_interest_compound_interval_sec(), '_rb_hbd_rows');
  DROP TABLE _rb_hbd_rows;

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
    SELECT d.account, d.nai,
      (array_agg(d.source_op ORDER BY d.updated_at DESC))[1],
      date_trunc('month', d.updated_at),
      (array_agg(d.balance ORDER BY d.updated_at DESC))[1],
      MIN(d.min_balance), MAX(d.max_balance)
    FROM saving_history_by_day d
    WHERE d.account IN (SELECT DISTINCT r.account FROM _rb_savings r)
    GROUP BY d.account, d.nai, date_trunc('month', d.updated_at);
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

  RAISE NOTICE 'negative control: no final check'; RETURN _rows;
  -- After the last read of HAF data: the source must still be complete from where it was at
  -- the start. Each statement above read a snapshot taken when it started (READ COMMITTED),
  -- and pruning only ever moves the first available block forward, so if it has not passed
  -- _needed now, no earlier statement read a pruned source.
  _gap := btracker_backend.cleared_accounts_source_gap(_context, _needed);
  IF _gap IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE = 'BT061',
      MESSAGE = format('btracker: HAF pruned blocks the null/steem.dao rebuild (#61) needed while it ran: %s', _gap);
  END IF;

  DROP TABLE _rb_clears;
  DROP TABLE _rb_balances;
  DROP TABLE _rb_savings;
  RETURN _rows;
END
$$;

-- Rebuild and record, unless btracker_repairs already holds the repair (a fresh install
-- records 'not needed' when it creates its tables). Callers make it safe to write:
--   * the repair_cleared_accounts procedure (installer) detaches the context group first;
--   * finalize_massive_sync_before_forking calls it while the context is non-forking.
-- The rebuild runs in a subtransaction: when it finds the source incomplete (SQLSTATE
-- BT061), before or during the run, everything it wrote is rolled back and nothing is
-- recorded, so the next install tries again. Returns whether it repaired.
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
  BEGIN
    _rows := btracker_backend.rebuild_cleared_accounts(_context);
  EXCEPTION WHEN SQLSTATE 'BT061' THEN
    RAISE WARNING '%. null/steem.dao NOT repaired: their data is left as it is (anything the rebuild wrote is rolled back) and the repair is not recorded, so the next install tries again. To rebuild them, resync btracker (uninstall, install) on a HAF that holds the full block history.', SQLERRM;
    RETURN FALSE;
  END;
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
CREATE OR REPLACE PROCEDURE btracker_backend.repair_cleared_accounts(_context TEXT, _commit BOOLEAN DEFAULT TRUE)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _group    TEXT[];  -- not hive.contexts_group: that domain rejects the NULL a declaration starts with
  _attached BOOLEAN;
  _gap      TEXT;
BEGIN
  IF EXISTS (SELECT 1 FROM btracker_repairs r WHERE r.name = 'cleared_accounts') THEN
    RETURN;
  END IF;
  IF NOT btracker_backend.history_indexes_exist(_context) THEN
    RAISE NOTICE 'btracker: null/steem.dao repair (#61) deferred to the end of massive sync';
    RETURN;
  END IF;
  -- checked before the detach, so a pruned HAF is left exactly as it is
  _gap := btracker_backend.cleared_accounts_source_gap(_context);
  IF _gap IS NOT NULL THEN
    RAISE WARNING 'btracker: null/steem.dao NOT repaired (#61): %. Their data is left as it is and the repair is not recorded, so the next install tries again. To rebuild them, resync btracker (uninstall, install) on a HAF that holds the full block history.', _gap;
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
