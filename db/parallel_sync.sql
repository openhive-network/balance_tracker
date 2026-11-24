/**
 * Concurrent massive sync
 * =======================
 *
 * The SQL half of scripts/btracker_parallel.py, haf_app_driver.py's range
 * processor when BTRACKER_SYNC_PARALLELISM is above 1 (scripts/process_blocks.sh).
 * Everything here is per schema: installed into btracker_app standalone and into
 * hafbe_bal under haf_block_explorer, and every entry point runs with its
 * search_path pinned to the schema it is installed in.
 *
 * For a massive-sync range the driver's transaction (the one whose commit moves
 * the HAF context) calls begin_parallel_range; then the batch is staged once into
 * shared tables (stage_shared_batch, on a worker connection, committed); then the
 * processor groups run at once on worker connections, each group in a transaction
 * of its own that commits its writes with its row in processor_group_progress
 * (process_processor_group); then finish_parallel_range, in the driver's
 * transaction, checks that every group applied the range and marks it applied, so
 * that btracker_process_blocks, which the registered entry procedure (btracker's
 * or haf_block_explorer's) still runs in that transaction, does not process it
 * again (applied_by_processor_groups). Live sync, and any range begin refuses,
 * keep the serial path unchanged.
 *
 * Crash safety: a group's writes and its progress row commit together, and the
 * driver's commit, which follows every group's in the WAL, deletes the progress
 * rows. A range whose driver transaction did not commit is delivered again from
 * the same first block; a group that committed it skips it, the others apply it.
 */

SET ROLE btracker_owner;

-- ============================================================================
-- PROCESSOR GROUPS
-- ============================================================================

/**
 * processor_group_progress
 * ------------------------
 * At most one row per group, and only while a batch is in flight: written in the
 * transaction that applies the batch to the group's tables, deleted in the one
 * that moves the HAF context past it. LOGGED, so it survives a crash exactly as
 * the group's writes do. Not registered with the context: a fork never rolls back
 * a massive-sync batch.
 */
CREATE TABLE IF NOT EXISTS processor_group_progress
(
  group_name  TEXT NOT NULL,
  first_block INT  NOT NULL,
  last_block  INT  NOT NULL,

  CONSTRAINT pk_processor_group_progress PRIMARY KEY (group_name)
);

/**
 * processor_group_processors()
 * ----------------------------
 * The processors of each group, in the order btracker_massive_processing calls
 * them. Groups are derived from what each processor reads and writes, so that no
 * group reads a table another group writes:
 *
 *   vests                  balances, rc_delegations, withdrawals, delegations.
 *                          rc_delegations reads the batch's VESTS changes balances
 *                          stages in the session temp table _btracker_vests_batch
 *                          and current_account_balances as of _to, and
 *                          account_withdraws, account_delegations and
 *                          current_accounts_delegations as of _from - 1, which
 *                          withdrawals and delegations write; withdrawals reads
 *                          current_account_balances and _btracker_vests_batch too.
 *   rewards                account_rewards, account_info_rewards
 *   account_vesting_stats  account_vesting_history (which it also reads), _by_day/_by_month
 *   transfer_stats         transfer_stats_by_hour/_day/_month
 *   orders                 order_state, order_lifecycle, order_stats_by_block(_group)
 *   converts               convert_state
 *   savings                account_savings(_history), transfer_saving_id, saving_history_by_*
 *   recurrent_transfers    recurrent_transfers
 *   vesting_stats          vesting_stats_by_day/_month
 *   escrows                escrow_state, escrow_fees
 *   proposal_payments      proposal_payments, proposal_payment_totals
 *
 * Every group reads the batch's operations and balance-clearing ops (the shared
 * staging below) and HAF's tables and views only otherwise. A processor added to
 * btracker_massive_processing needs a place here, by the same derivation; the
 * equality test (tests/parallel_sync) fails while it has none.
 *
 * Listed longest first (steem-20, 30-67M: vests ~1.6 s, rewards 1.3 s,
 * account_vesting_stats 0.9 s, the rest 0.1-0.15 s each): scripts/btracker_parallel.py
 * deals the groups out to its connections in this order.
 */
CREATE OR REPLACE FUNCTION processor_group_processors()
RETURNS TABLE (group_name TEXT, processors TEXT[])
LANGUAGE sql IMMUTABLE
AS
$$
  SELECT g.group_name, g.processors
  FROM (VALUES
    (1,  'vests', ARRAY['process_block_range_balances', 'process_block_range_rc_delegations',
                        'process_block_range_withdrawals', 'process_block_range_delegations']),
    (2,  'rewards', ARRAY['process_block_range_rewards']),
    (3,  'account_vesting_stats', ARRAY['process_account_vesting_stats']),
    (4,  'transfer_stats', ARRAY['process_transfer_stats']),
    (5,  'orders', ARRAY['process_block_range_orders']),
    (6,  'converts', ARRAY['process_block_range_converts']),
    (7,  'savings', ARRAY['process_block_range_savings']),
    (8,  'recurrent_transfers', ARRAY['process_block_range_recurrent_transfers']),
    (9,  'vesting_stats', ARRAY['process_vesting_stats']),
    (10, 'escrows', ARRAY['process_block_range_escrows']),
    (11, 'proposal_payments', ARRAY['process_block_range_proposal_payments'])
  ) AS g (position, group_name, processors)
  ORDER BY g.position;
$$;

/**
 * processor_groups()
 * ------------------
 * The group names, longest first (processor_group_processors).
 */
CREATE OR REPLACE FUNCTION processor_groups()
RETURNS TEXT[]
LANGUAGE sql IMMUTABLE
AS
$$
  SELECT array_agg(p.group_name ORDER BY p.position)
  FROM processor_group_processors() WITH ORDINALITY AS p (group_name, processors, position);
$$;

/**
 * processor_group_from()
 * ----------------------
 * The first block of _first.._last that _group has still to apply, or NULL when
 * it has applied all of them. A batch is delivered again from the block it
 * started at, so a progress row for another first block, or reaching past _last,
 * means the HAF context was moved under an in-flight batch: that raises rather
 * than apply a block twice or skip one.
 */
CREATE OR REPLACE FUNCTION processor_group_from(_group TEXT, _first INT, _last INT)
RETURNS INT
LANGUAGE plpgsql STABLE
AS
$$
DECLARE
  __done processor_group_progress;
BEGIN
  SELECT * INTO __done FROM processor_group_progress WHERE group_name = _group;
  IF NOT FOUND THEN
    RETURN _first;
  END IF;
  IF __done.first_block <> _first OR __done.last_block > _last THEN
    RAISE EXCEPTION 'btracker: processor group % has applied blocks %..% of a batch that was not committed, which blocks %..% do not continue',
      _group, __done.first_block, __done.last_block, _first, _last
      USING HINT = 'The HAF context was moved while a batch was in flight. Resync, or delete the processor_group_progress row once the group''s tables hold exactly the blocks up to the context''s.';
  END IF;
  IF __done.last_block = _last THEN
    RETURN NULL;
  END IF;
  RETURN __done.last_block + 1;
END
$$;

/**
 * run_processor_group()
 * ---------------------
 * _group's processors for _from.._to, in order, each followed by the per-stage
 * timing NOTICE btracker_massive_processing writes. The caller has made the
 * batch's operations readable as _btracker_ops_batch and _btracker_clears_batch.
 */
CREATE OR REPLACE FUNCTION run_processor_group(_group TEXT, _from INT, _to INT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __processors TEXT[] := (SELECT p.processors FROM processor_group_processors() p WHERE p.group_name = _group);
  __processor TEXT;
  __stage_ts TIMESTAMPTZ;
BEGIN
  IF __processors IS NULL THEN
    RAISE EXCEPTION 'btracker: unknown processor group %', _group;
  END IF;

  FOREACH __processor IN ARRAY __processors LOOP
    __stage_ts := clock_timestamp();
    EXECUTE format('SELECT %I($1, $2)', __processor) USING _from, _to;
    RAISE NOTICE 'btracker stage %: % s', regexp_replace(__processor, '^process_(block_range_)?', ''),
      round(EXTRACT(epoch FROM clock_timestamp() - __stage_ts)::numeric, 3);
  END LOOP;
END
$$;

-- ============================================================================
-- SHARED BATCH STAGING
-- ============================================================================
-- A batch is staged once, into UNLOGGED tables every worker connection reads,
-- rather than into each session's temp tables (a temp table is invisible to the
-- other sessions). There are two slots, so that the next batch can be staged
-- while the groups of the current one read theirs: shared_batch_ops_<slot> has
-- the columns of _btracker_ops_batch, shared_batch_clears_<slot> those of
-- _btracker_clears_batch. shared_batch_marker holds a row per slot whose staging
-- is complete, committed with its rows, and a slot is read only for the range in
-- its row. The marker is UNLOGGED like the rows: the crash recovery that empties
-- them empties it too, and the batch is staged again. A group attaches a slot
-- (attach_shared_batch): in its transaction _btracker_ops_batch and
-- _btracker_clears_batch are temp views of the slot's tables, which the process_*
-- functions read as they read the temp tables (pg_temp is searched first).
--
-- The staging reads hafd.operations and hafd.accounts, never the context's views:
-- when HAF attaches or detaches the context, the driver's transaction recreates
-- those views and holds them locked while it waits for the staging, which would
-- then wait for it. For a range up to HAF's irreversible block (a range past it
-- is not staged) they return exactly what the detached context's operations_view
-- and accounts_view do, which is what btracker_prefetch_operations reads during
-- massive sync. These tables are the only UNLOGGED ones in the schema; none holds
-- synced state.
-- ============================================================================

CREATE UNLOGGED TABLE IF NOT EXISTS shared_batch_marker
(
  slot        SMALLINT NOT NULL,
  first_block INT      NOT NULL,
  last_block  INT      NOT NULL,

  CONSTRAINT pk_shared_batch_marker PRIMARY KEY (slot),
  CONSTRAINT ck_shared_batch_marker_slot CHECK (slot IN (0, 1))
);

CREATE UNLOGGED TABLE IF NOT EXISTS shared_batch_ops_0 WITH (autovacuum_enabled = FALSE) AS
  SELECT id, block_num, trx_in_block, op_pos, op_type_id, body_value, custom_json_type_id
  FROM operations_view
  WITH NO DATA;

CREATE UNLOGGED TABLE IF NOT EXISTS shared_batch_ops_1 WITH (autovacuum_enabled = FALSE) AS
  SELECT id, block_num, trx_in_block, op_pos, op_type_id, body_value, custom_json_type_id
  FROM operations_view
  WITH NO DATA;

CREATE UNLOGGED TABLE IF NOT EXISTS shared_batch_clears_0
(
  account_id      INT,
  source_op       BIGINT,
  op_type_id      SMALLINT,
  source_op_block INT
) WITH (autovacuum_enabled = FALSE);

CREATE UNLOGGED TABLE IF NOT EXISTS shared_batch_clears_1
(
  account_id      INT,
  source_op       BIGINT,
  op_type_id      SMALLINT,
  source_op_block INT
) WITH (autovacuum_enabled = FALSE);

/**
 * shared_batch_slot()
 * -------------------
 * The slot to stage _first.._last in: the one already holding the range;
 * otherwise one that does not hold the range ending just before it (the batch
 * whose groups may still be reading it), preferring an empty slot.
 */
CREATE OR REPLACE FUNCTION shared_batch_slot(_first INT, _last INT)
RETURNS SMALLINT
LANGUAGE sql STABLE
AS
$$
  SELECT s.slot
  FROM (VALUES (0::SMALLINT), (1::SMALLINT)) AS s (slot)
  ORDER BY
    EXISTS (SELECT FROM shared_batch_marker m
            WHERE m.slot = s.slot AND m.first_block = _first AND m.last_block = _last) DESC,
    EXISTS (SELECT FROM shared_batch_marker m WHERE m.slot = s.slot AND m.last_block = _first - 1),
    EXISTS (SELECT FROM shared_batch_marker m WHERE m.slot = s.slot),
    s.slot
  LIMIT 1;
$$;

/**
 * shared_batch_complete()
 * -----------------------
 * Whether a slot holds the complete staging of exactly _first.._last.
 */
CREATE OR REPLACE FUNCTION shared_batch_complete(_first INT, _last INT)
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS
$$
  SELECT EXISTS (SELECT FROM shared_batch_marker m WHERE m.first_block = _first AND m.last_block = _last);
$$;

/**
 * stage_shared_batch()
 * --------------------
 * Stages _first.._last into a slot, in a transaction of its own that the caller
 * commits. A no-op when the range reaches past HAF's irreversible block, when
 * every group has applied part of the batch (nothing would read it), or when a
 * slot already holds the range (a batch delivered again, or staged ahead).
 * Deleting the slot's marker row first waits for every group still reading the
 * slot (attach_shared_batch locks it FOR SHARE); TRUNCATE then empties the slot in
 * this transaction, so no other session sees it half filled, and the ops are
 * analyzed as btracker_prefetch_operations analyzes the temp table.
 */
CREATE OR REPLACE FUNCTION stage_shared_batch(_first INT, _last INT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __op_clear_null  INT := btracker_backend.op_clear_null_account_balance();
  __op_consolidate INT := btracker_backend.op_consolidate_treasury_balance();
  __stage_ts TIMESTAMPTZ := clock_timestamp();
  __slot SMALLINT;
  __ops TEXT;
  __clears TEXT;
BEGIN
  IF _last > hive.app_get_irreversible_block() THEN
    RETURN;
  END IF;
  -- Progress rows for _first are the groups that have applied this batch; rows
  -- for another first block are the batch in flight while this one is staged ahead.
  IF NOT EXISTS (
    SELECT FROM unnest(processor_groups()) AS g (name)
    WHERE NOT EXISTS (SELECT FROM processor_group_progress p WHERE p.group_name = g.name AND p.first_block = _first)
  ) THEN
    RETURN;
  END IF;
  -- One staging at a time chooses and fills a slot: a coordinator that died can leave one running.
  PERFORM pg_advisory_xact_lock(hashtext(current_schema() || '.shared_batch_marker'));
  __slot := shared_batch_slot(_first, _last);
  IF EXISTS (SELECT FROM shared_batch_marker m WHERE m.slot = __slot AND m.first_block = _first AND m.last_block = _last) THEN
    RETURN;
  END IF;

  PERFORM set_config('synchronous_commit', 'OFF', true);
  __ops := format('%I', 'shared_batch_ops_' || __slot);
  __clears := format('%I', 'shared_batch_clears_' || __slot);
  DELETE FROM shared_batch_marker WHERE slot = __slot;
  EXECUTE format('TRUNCATE %s, %s', __ops, __clears);

  EXECUTE format(
    'INSERT INTO %s
     SELECT ho.id, hafd.operation_id_to_block_num(ho.id), ho.trx_in_block, ho.op_pos, ho.op_type_id,
            ho.body_value, ho.custom_json_type_id
     FROM hafd.operations ho
     WHERE hafd.operation_id_to_block_num(ho.id) BETWEEN $1 AND $2', __ops)
  USING _first, _last;
  EXECUTE format('ANALYZE %s', __ops);

  -- btracker_backend.prefetch_account_clears' probe of a massive batch.
  EXECUTE format(
    'INSERT INTO %s
     SELECT r.account_id, r.source_op, r.op_type_id, r.source_op_block
     FROM (
       SELECT
         CASE WHEN ho.op_type_id = $3
           THEN (SELECT ha.id FROM hafd.accounts ha WHERE ha.name = ''null'')
           ELSE (SELECT ha.id FROM hafd.accounts ha WHERE ha.name = ''steem.dao'')
         END::INT AS account_id,
         ho.id::BIGINT AS source_op,
         ho.op_type_id::SMALLINT AS op_type_id,
         hafd.operation_id_to_block_num(ho.id)::INT AS source_op_block
       FROM hafd.operations ho
       WHERE ho.op_type_id IN ($3, $4)
         AND hafd.operation_id_to_block_num(ho.id) BETWEEN $1 AND $2
     ) r
     WHERE r.account_id IS NOT NULL', __clears)
  USING _first, _last, __op_clear_null, __op_consolidate;

  INSERT INTO shared_batch_marker (slot, first_block, last_block) VALUES (__slot, _first, _last);

  RAISE NOTICE 'btracker stage shared staging <%, %> in slot %: % s', _first, _last, __slot,
    round(EXTRACT(epoch FROM clock_timestamp() - __stage_ts)::numeric, 3);
END
$$;

/**
 * attach_shared_batch()
 * ---------------------
 * In the calling transaction, makes the slot holding exactly _from.._to this
 * session's _btracker_ops_batch and _btracker_clears_batch (temp views, after
 * dropping any temp tables of those names). Returns FALSE, having done nothing,
 * when no slot holds that range. FOR SHARE keeps the slot from being restaged
 * until the transaction ends.
 */
CREATE OR REPLACE FUNCTION attach_shared_batch(_from INT, _to INT)
RETURNS BOOLEAN
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __slot SMALLINT;
BEGIN
  SELECT m.slot INTO __slot
  FROM shared_batch_marker m
  WHERE m.first_block = _from AND m.last_block = _to
  ORDER BY m.slot
  LIMIT 1
  FOR SHARE;
  IF NOT FOUND THEN
    RETURN FALSE;
  END IF;

  PERFORM btracker_reset_batch_table('_btracker_ops_batch', FALSE);
  PERFORM btracker_reset_batch_table('_btracker_clears_batch', FALSE);
  EXECUTE format('CREATE TEMP VIEW _btracker_ops_batch AS SELECT * FROM %I.%I', current_schema(), 'shared_batch_ops_' || __slot);
  EXECUTE format('CREATE TEMP VIEW _btracker_clears_batch AS SELECT * FROM %I.%I', current_schema(), 'shared_batch_clears_' || __slot);
  RETURN TRUE;
END
$$;

CREATE OR REPLACE FUNCTION detach_shared_batch()
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
BEGIN
  DROP VIEW pg_temp._btracker_ops_batch;
  DROP VIEW pg_temp._btracker_clears_batch;
END
$$;

/**
 * release_shared_batch()
 * ----------------------
 * Empties both slots once massive sync is over: they would otherwise keep the
 * last two batches' operations for good.
 */
CREATE OR REPLACE FUNCTION release_shared_batch()
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
BEGIN
  IF EXISTS (SELECT FROM shared_batch_marker)
     OR pg_relation_size('shared_batch_ops_0') + pg_relation_size('shared_batch_ops_1')
      + pg_relation_size('shared_batch_clears_0') + pg_relation_size('shared_batch_clears_1') > 0 THEN
    DELETE FROM shared_batch_marker;
    TRUNCATE shared_batch_ops_0, shared_batch_ops_1, shared_batch_clears_0, shared_batch_clears_1;
  END IF;
END
$$;

-- ============================================================================
-- ENTRY POINTS
-- ============================================================================

/**
 * process_processor_group()
 * -------------------------
 * _group's share of the batch _first.._last, on a worker connection, in a
 * transaction of its own that the caller commits: the group's writes and its
 * progress row commit together. A no-op when the group has already applied the
 * batch. Reads the shared staging when a slot holds exactly the blocks the group
 * has still to apply, and otherwise stages them in this session's temp tables
 * (btracker_prefetch_operations, which reads the context's views: the driver's
 * transaction holds no lock on them while groups run, begin_parallel_range).
 */
CREATE OR REPLACE FUNCTION process_processor_group(_group TEXT, _first INT, _last INT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __from INT;
  __shared BOOLEAN;
BEGIN
  IF NOT _group = ANY(processor_groups()) THEN
    RAISE EXCEPTION 'btracker: unknown processor group %', _group;
  END IF;
  -- Serialises two calls for one group (a worker of a coordinator that died, still
  -- running when the batch is delivered again): the second reads the first's row.
  PERFORM pg_advisory_xact_lock(hashtext(current_schema() || '.processor_group_progress'), hashtext(_group));
  __from := processor_group_from(_group, _first, _last);
  IF __from IS NULL THEN
    RETURN;
  END IF;

  -- The massive-sync durability trade btracker_massive_processing makes. The
  -- driver's commit follows this one in the WAL, so a crash cannot keep the
  -- context's move and lose this group's writes.
  PERFORM set_config('synchronous_commit', 'OFF', true);
  __shared := attach_shared_batch(__from, _last);
  IF NOT __shared THEN
    PERFORM btracker_prefetch_operations(__from, _last);
  END IF;

  PERFORM run_processor_group(_group, __from, _last);

  IF __shared THEN
    PERFORM detach_shared_batch();
  END IF;
  INSERT INTO processor_group_progress (group_name, first_block, last_block)
  VALUES (_group, _first, _last)
  ON CONFLICT (group_name) DO UPDATE SET last_block = EXCLUDED.last_block;
END
$$;

/**
 * begin_parallel_range()
 * ----------------------
 * In the driver's transaction, before the groups: whether _first.._last is
 * processed by the processor groups. FALSE, having done nothing else, leaves the
 * range to the serial path (the registered entry procedure):
 *   - outside massive sync (is_massive_sync_pending), where it also empties the
 *     shared staging;
 *   - while the context is attached (massive processing in the LIVE stage before
 *     finalize): its views then show only blocks up to the context's committed
 *     current block, which the worker connections see;
 *   - for the batch that holds HF23, which btracker_massive_processing splits
 *     around btracker_backend.process_hf_23;
 *   - when this transaction holds a lock on a relation of the schema that the
 *     workers' reads or writes would wait for (HAF detaching the context in it
 *     recreates the views and drops the tables' triggers): the driver waits for
 *     the workers, so they would never get it.
 */
CREATE OR REPLACE FUNCTION begin_parallel_range(_first INT, _last INT)
RETURNS BOOLEAN
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __context hive.context_name := current_schema();
  __hardfork_23_block INT := (SELECT block_num FROM hafd.applied_hardforks WHERE hardfork_num = 23);
BEGIN
  IF NOT is_massive_sync_pending(__context) THEN
    PERFORM release_shared_batch();
    RETURN FALSE;
  END IF;

  IF hive.app_context_is_attached(__context)
     OR COALESCE(__hardfork_23_block BETWEEN _first AND _last, FALSE)
     OR EXISTS (
       SELECT FROM pg_locks l
       JOIN pg_class c ON c.oid = l.relation
       WHERE l.pid = pg_backend_pid()
         AND l.locktype = 'relation'
         AND l.granted
         AND c.relnamespace = current_schema()::regnamespace
         AND l.mode IN ('ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock')
     ) THEN
    RETURN FALSE;
  END IF;

  -- Session-scoped, as btracker_massive_processing sets it.
  PERFORM set_config('synchronous_commit', 'OFF', false);
  RETURN TRUE;
END
$$;

/**
 * finish_parallel_range()
 * -----------------------
 * In the driver's transaction, once every group has committed: raises unless
 * every group has applied _first.._last, so the driver's commit (which moves the
 * HAF context) can only follow a fully applied batch. Then deletes the progress
 * rows and the marker of every slot the batch moves the context past (the next
 * range, staged ahead, stays), and marks the range applied for this transaction
 * (applied_by_processor_groups).
 */
CREATE OR REPLACE FUNCTION finish_parallel_range(_first INT, _last INT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __group TEXT;
BEGIN
  FOREACH __group IN ARRAY processor_groups() LOOP
    IF processor_group_from(__group, _first, _last) IS NOT NULL THEN
      RAISE EXCEPTION 'btracker: processor group % has not applied blocks %..%; the batch is not committed', __group, _first, _last;
    END IF;
  END LOOP;

  DELETE FROM processor_group_progress;
  DELETE FROM shared_batch_marker WHERE first_block <= _last;
  PERFORM set_config('btracker.parallel_applied_range', format('%s:%s:%s', current_schema(), _first, _last), true);
END
$$;

/**
 * applied_by_processor_groups()
 * -----------------------------
 * Called by btracker_process_blocks: TRUE when this transaction's
 * finish_parallel_range applied _block_range of context _context_name, which
 * btracker_process_blocks then does not process again. Raises when processor
 * groups have applied part of a batch that was not committed: processing a range
 * serially then would apply their blocks twice.
 */
CREATE OR REPLACE FUNCTION applied_by_processor_groups(_context_name hive.context_name, _block_range hive.blocks_range)
RETURNS BOOLEAN
LANGUAGE plpgsql STABLE
AS
$$
BEGIN
  IF current_setting('btracker.parallel_applied_range', true)
     = format('%s:%s:%s', _context_name, _block_range.first_block, _block_range.last_block) THEN
    RETURN TRUE;
  END IF;

  IF EXISTS (SELECT FROM processor_group_progress) THEN
    RAISE EXCEPTION 'btracker: processor groups have applied part of a batch that was not committed: %',
      (SELECT string_agg(format('%s %s..%s', p.group_name, p.first_block, p.last_block), ', ' ORDER BY p.group_name) FROM processor_group_progress p)
      USING HINT = 'Run block processing with BTRACKER_SYNC_PARALLELISM above 1 (scripts/btracker_parallel.py) until that batch is committed.';
  END IF;
  RETURN FALSE;
END
$$;

-- The entry points the coordinator calls on its own connections, and the two the
-- driver's transaction calls, resolve the schema's tables and functions whoever
-- calls them; pg_temp, not named, is still searched first for relations.
DO $$
DECLARE
  __schema_name TEXT := current_schema();
  __function TEXT;
BEGIN
  FOREACH __function IN ARRAY ARRAY[
    'processor_groups()',
    'stage_shared_batch(INT, INT)',
    'shared_batch_complete(INT, INT)',
    'process_processor_group(TEXT, INT, INT)',
    'begin_parallel_range(INT, INT)',
    'finish_parallel_range(INT, INT)'
  ] LOOP
    EXECUTE format('ALTER FUNCTION %I.%s SET search_path = %I, pg_catalog', __schema_name, __function, __schema_name);
  END LOOP;
END
$$;

RESET ROLE;
