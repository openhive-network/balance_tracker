/*
 * order_lifecycle.sql
 * ===================
 * Limit orders kept with their outcome (process_block_range_orders writes
 * order_lifecycle next to order_state) and the two endpoints that count them,
 * /order-stats and /accounts/{account-name}/order-stats:
 *
 *   - after the massive sync: one row per limit_order_create(2) operation of the
 *     synced blocks, the open rows equal order_state, over the whole range
 *     created = filled + cancelled + open, and every row equals a replay of the
 *     synced order operations one at a time (closing the HF23-cleared accounts'
 *     open orders at HF23 when the synced blocks reach it);
 *   - synthetic order operations in the three blocks before the synced head: a
 *     two-block range processed as massive sync does, then the next block as a
 *     live block. Covers create -> fill, create -> partial fill -> cancel,
 *     expiry (a virtual limit_order_cancelled only, also after a fill in the
 *     same block), several fills across both calls, a user cancel followed by
 *     its virtual limit_order_cancelled (also in the transaction of a fill), a
 *     dust cull (hived's limit_order_cancelled after a fill in its transaction,
 *     an order hived counts as filled) of an order created in the range and of
 *     one open from the earlier call, an order id reused within a range and
 *     across the two, and an order id reused within one transaction (the owner
 *     cancels it and re-creates it, which hived culls at once; hived culls it
 *     and the owner re-creates and cancels it). Every row, order_state against
 *     the open rows, the replay of the same operations, and the endpoints'
 *     windows are checked;
 *   - after HAF rolls the context back (hive.context_back_from_fork) the
 *     synthetic rows are gone and the synced ones are back (skipped when the
 *     context is not forking);
 *   - after the massive sync, after the synthetic blocks and after the fork
 *     rollback: order_stats_by_block and order_stats_by_block_group count what
 *     order_lifecycle holds per creation block and group, and /order-stats,
 *     summed from them, equals order_lifecycle's counts over block and date
 *     windows (unbounded, across group edges, inside one group, exactly one
 *     group, holding orders that closed after the window, a date window),
 *     while an inverted block or date window gets HAF's error from both
 *     endpoints;
 *   - a pretend HF23 block clearing one synthetic account: process_hf_23 closes
 *     its open orders as cancelled there, and the replay agrees, also when it
 *     ends in that block.
 *   - an upgrade from a version without the rollups: both dropped and
 *     db/btracker_app.sql run again, they equal order_lifecycle's counts, and
 *     still do after a block that fills and cancels orders created before it;
 *   - the same upgrade with three processed blocks past the context's
 *     irreversible block (skipped when the context is not forking): the rollups
 *     equal order_lifecycle's counts after it, after a fork back to the first of
 *     those blocks, after a fork back to the irreversible block, and after a
 *     different chain processed past it.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Everything after the first check runs in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- _btracker_ops_batch (what btracker_prefetch_operations gives the processing)
-- holding one operation per element of _ops: {"block", "pos", "op_type_id", "body"},
-- and "trx" (trx_in_block) for an operation in a transaction, block level (-1) without it.
CREATE FUNCTION pg_temp.stage_ops(_ops JSONB)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT hafd.operation_id((o ->> 'block')::INT, (o ->> 'pos')::INT) AS id,
         (o ->> 'block')::INT AS block_num, COALESCE((o ->> 'trx')::SMALLINT, -1::SMALLINT) AS trx_in_block, (o ->> 'pos')::INT AS op_pos,
         (o ->> 'op_type_id')::SMALLINT AS op_type_id,
         o -> 'body' AS body_value,
         NULL::INT AS custom_json_type_id
  FROM jsonb_array_elements(_ops) AS o;
END
$f$;

CREATE FUNCTION pg_temp.asset(_amount BIGINT, _nai SMALLINT)
RETURNS JSONB LANGUAGE sql IMMUTABLE
AS $f$
  SELECT jsonb_build_object('amount', _amount::TEXT, 'precision', 3, 'nai', '@@0000000' || _nai::TEXT)
$f$;

CREATE FUNCTION pg_temp.op_create(_block INT, _pos INT, _owner TEXT, _order_id BIGINT, _amount BIGINT, _nai SMALLINT, _create2 BOOLEAN = FALSE)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('block', _block, 'pos', _pos,
    'op_type_id', CASE WHEN _create2 THEN btracker_backend.op_limit_order_create2() ELSE btracker_backend.op_limit_order_create() END,
    'body', jsonb_build_object('owner', _owner, 'orderid', _order_id, 'amount_to_sell', pg_temp.asset(_amount, _nai)))
$f$;

-- A user cancel (limit_order_cancel) or, with _virtual, hived's limit_order_cancelled
-- (expiry, or the virtual operation that follows a user cancel).
CREATE FUNCTION pg_temp.op_cancel(_block INT, _pos INT, _owner TEXT, _order_id BIGINT, _virtual BOOLEAN)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('block', _block, 'pos', _pos,
    'op_type_id', CASE WHEN _virtual THEN btracker_backend.op_limit_order_cancelled() ELSE btracker_backend.op_limit_order_cancel() END,
    'body', CASE WHEN _virtual
                 THEN jsonb_build_object('seller', _owner, 'orderid', _order_id, 'amount_back', pg_temp.asset(1, 21::SMALLINT))
                 ELSE jsonb_build_object('owner', _owner, 'orderid', _order_id) END)
$f$;

-- A fill_order of the open order, and of the current (taker) order when _current_owner is given.
CREATE FUNCTION pg_temp.op_fill(_block INT, _pos INT, _open_owner TEXT, _open_order_id BIGINT, _open_pays BIGINT, _open_nai SMALLINT,
                                _current_owner TEXT = NULL, _current_order_id BIGINT = NULL, _current_pays BIGINT = NULL, _current_nai SMALLINT = NULL)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('block', _block, 'pos', _pos, 'op_type_id', btracker_backend.op_fill_order(),
    'body', jsonb_strip_nulls(jsonb_build_object(
      'open_owner', _open_owner, 'open_orderid', _open_order_id, 'open_pays', pg_temp.asset(_open_pays, _open_nai),
      'current_owner', _current_owner, 'current_orderid', _current_order_id,
      'current_pays', CASE WHEN _current_owner IS NOT NULL THEN pg_temp.asset(_current_pays, _current_nai) END)))
$f$;

-- _op in the block's transaction _trx.
CREATE FUNCTION pg_temp.in_trx(_trx INT, _op JSONB)
RETURNS JSONB LANGUAGE sql IMMUTABLE
AS $f$
  SELECT _op || jsonb_build_object('trx', _trx)
$f$;

-- order_lifecycle's open rows against order_state, both ways.
CREATE FUNCTION pg_temp.check_open_rows_equal_order_state(_when TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __only_lifecycle BIGINT;
  __only_state BIGINT;
BEGIN
  SELECT count(*) INTO __only_lifecycle FROM (
    SELECT owner_id, order_id, nai, remaining, block_created FROM order_lifecycle WHERE end_block IS NULL
    EXCEPT ALL
    SELECT owner_id, order_id, nai, remaining, block_created FROM order_state) d;
  SELECT count(*) INTO __only_state FROM (
    SELECT owner_id, order_id, nai, remaining, block_created FROM order_state
    EXCEPT ALL
    SELECT owner_id, order_id, nai, remaining, block_created FROM order_lifecycle WHERE end_block IS NULL) d;
  IF __only_lifecycle <> 0 OR __only_state <> 0 THEN
    RAISE EXCEPTION '%: % open order_lifecycle rows not in order_state, % order_state rows not open in order_lifecycle',
      _when, __only_lifecycle, __only_state;
  END IF;
  RAISE NOTICE 'PASS: %: the open order_lifecycle rows equal order_state (% orders)', _when, (SELECT count(*) FROM order_state);
END
$f$;

CREATE FUNCTION pg_temp.stats(_stats btracker_backend.order_stats)
RETURNS JSONB LANGUAGE sql IMMUTABLE
AS $f$
  SELECT to_jsonb(_stats)
$f$;

-- The rollups against order_lifecycle, and /order-stats against order_lifecycle's
-- counts over windows around the middle of the synced blocks and their end.
CREATE FUNCTION pg_temp.check_order_stats_rollup(_when TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __head CONSTANT INT := hive.app_get_current_block_num(current_setting('btracker_test.schema'));
  __group_blocks CONSTANT INT := btracker_backend.order_stats_group_blocks();
  __mid CONSTANT INT := __head / 2 / __group_blocks * __group_blocks;
  __mid_at CONSTANT TIMESTAMP := (SELECT bv.created_at FROM hive.blocks_view bv WHERE bv.num = __mid);
  __mismatched BIGINT;
  __example TEXT;
  __window RECORD;
  __range hive.blocks_range;
  __expected JSONB;
  __actual JSONB;
  __closed_after BIGINT := 0;
  __windows INT := 0;
BEGIN
  SELECT count(*), min(d::TEXT) INTO __mismatched, __example FROM (
    (SELECT block_created, created, filled, cancelled FROM order_stats_by_block
     EXCEPT
     SELECT block_created, count(*), count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
     FROM order_lifecycle GROUP BY block_created)
    UNION ALL
    (SELECT block_created, count(*), count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
     FROM order_lifecycle GROUP BY block_created
     EXCEPT
     SELECT block_created, created, filled, cancelled FROM order_stats_by_block)) d;
  IF __mismatched <> 0 THEN
    RAISE EXCEPTION '%: % order_stats_by_block rows differ from order_lifecycle per creation block, e.g. %', _when, __mismatched, __example;
  END IF;

  SELECT count(*), min(d::TEXT) INTO __mismatched, __example FROM (
    (SELECT block_group, created, filled, cancelled FROM order_stats_by_block_group
     EXCEPT
     SELECT block_created / __group_blocks, count(*), count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
     FROM order_lifecycle GROUP BY block_created / __group_blocks)
    UNION ALL
    (SELECT block_created / __group_blocks, count(*), count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
     FROM order_lifecycle GROUP BY block_created / __group_blocks
     EXCEPT
     SELECT block_group, created, filled, cancelled FROM order_stats_by_block_group)) d;
  IF __mismatched <> 0 THEN
    RAISE EXCEPTION '%: % order_stats_by_block_group rows differ from order_lifecycle per block group, e.g. %', _when, __mismatched, __example;
  END IF;
  RAISE NOTICE 'PASS: %: order_stats_by_block (% blocks) and order_stats_by_block_group (% groups) count order_lifecycle',
    _when, (SELECT count(*) FROM order_stats_by_block), (SELECT count(*) FROM order_stats_by_block_group);

  FOR __window IN
    SELECT w.from_block, w.to_block FROM (VALUES
      (NULL::TEXT, NULL::TEXT),
      (NULL, __mid::TEXT),
      ((__mid + 1)::TEXT, NULL),
      ((__mid / 2 + 7)::TEXT, (__mid + __mid / 2 - 3)::TEXT),
      ((__mid - 5)::TEXT, (__mid + 5)::TEXT),
      ((__mid + 3)::TEXT, (__mid + 8)::TEXT),
      (__mid::TEXT, (__mid + __group_blocks - 1)::TEXT),
      ((__mid - __group_blocks)::TEXT, (__mid + __group_blocks)::TEXT),
      ((__head - 3 * __group_blocks + 1)::TEXT, __head::TEXT),
      ((__mid_at - INTERVAL '10 days')::TEXT, __mid_at::TEXT)
    ) AS w(from_block, to_block)
  LOOP
    __range := hive.convert_to_blocks_range(__window.from_block, __window.to_block);
    SELECT jsonb_build_object(
             'created', count(*),
             'filled', count(*) FILTER (WHERE ol.outcome = 'filled'),
             'cancelled', count(*) FILTER (WHERE ol.outcome = 'cancelled'),
             'open', count(*) FILTER (WHERE ol.outcome IS NULL),
             'fill_rate_pct', CASE WHEN count(*) > 0
                                   THEN round(100.0 * count(*) FILTER (WHERE ol.outcome = 'filled') / count(*), 2)::FLOAT END),
           count(*) FILTER (WHERE ol.end_block > __range.last_block)
    INTO __expected, __mismatched
    FROM order_lifecycle ol
    WHERE ol.block_created BETWEEN COALESCE(__range.first_block, 0) AND COALESCE(__range.last_block, 2147483647);
    __actual := pg_temp.stats(btracker_endpoints.get_order_stats(__window.from_block, __window.to_block));
    IF __actual IS DISTINCT FROM __expected THEN
      RAISE EXCEPTION '%: /order-stats from % to %: expected order_lifecycle''s %, found %',
        _when, __window.from_block, __window.to_block, __expected, __actual;
    END IF;
    __closed_after := __closed_after + __mismatched;
    __windows := __windows + 1;
  END LOOP;
  IF __closed_after = 0 THEN
    RAISE EXCEPTION '%: no window holds an order that closed after it', _when;
  END IF;
  RAISE NOTICE 'PASS: %: /order-stats equals order_lifecycle''s counts over % windows (% orders closed after their window)',
    _when, __windows, __closed_after;

  FOR __window IN
    SELECT w.from_block, w.to_block FROM (VALUES
      ((__mid + 1)::TEXT, __mid::TEXT),
      ((__mid_at + INTERVAL '1 day')::TEXT, __mid_at::TEXT)
    ) AS w(from_block, to_block)
  LOOP
    BEGIN
      PERFORM btracker_endpoints.get_order_stats(__window.from_block, __window.to_block);
      RAISE EXCEPTION '%: /order-stats from % to % did not fail', _when, __window.from_block, __window.to_block;
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM NOT LIKE 'The starting % must be % the ending %' THEN
        RAISE;
      END IF;
    END;
    BEGIN
      PERFORM btracker_endpoints.get_account_order_stats(
        (SELECT av.name FROM hive.accounts_view av ORDER BY av.id LIMIT 1), __window.from_block, __window.to_block);
      RAISE EXCEPTION '%: /accounts/{account-name}/order-stats from % to % did not fail', _when, __window.from_block, __window.to_block;
    EXCEPTION WHEN raise_exception THEN
      IF SQLERRM NOT LIKE 'The starting % must be % the ending %' THEN
        RAISE;
      END IF;
    END;
  END LOOP;
  RAISE NOTICE 'PASS: %: both endpoints reject a window whose from-block is after its to-block', _when;
END
$f$;

-- After the massive sync.
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __head CONSTANT INT := hive.app_get_current_block_num(__schema);
  __creates BIGINT;
  __rows BIGINT := (SELECT count(*) FROM order_lifecycle);
  __stats btracker_backend.order_stats := btracker_endpoints.get_order_stats();
BEGIN
  SELECT count(*) INTO __creates
  FROM operations_view o
  WHERE o.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2())
    AND o.block_num <= __head;

  IF __rows = 0 OR __rows <> __creates THEN
    RAISE EXCEPTION 'expected one order_lifecycle row per limit order create up to block % (%), found %', __head, __creates, __rows;
  END IF;
  RAISE NOTICE 'PASS: one order_lifecycle row per limit order create (% orders)', __rows;

  PERFORM pg_temp.check_open_rows_equal_order_state('after the massive sync');

  IF __stats.created <> __rows
     OR __stats.created <> __stats.filled + __stats.cancelled + __stats.open
     OR __stats.open <> (SELECT count(*) FROM order_state) THEN
    RAISE EXCEPTION '/order-stats over the synced blocks: % for % orders, % open in order_state',
      pg_temp.stats(__stats), __rows, (SELECT count(*) FROM order_state);
  END IF;
  RAISE NOTICE 'PASS: /order-stats over the synced blocks: created = filled + cancelled + open: %', pg_temp.stats(__stats);

  PERFORM pg_temp.check_order_stats_rollup('after the massive sync');
END
$$;

-- A replay of the order operations in _replay_ops (id, block_num, trx_in_block,
-- op_type_id, body) one by one, in operation order, reading the operation bodies
-- directly, into replay_orders. A virtual limit_order_cancelled in a transaction
-- whose last earlier event of the order in that transaction is a fill is hived's
-- dust cull of a fully filled order: the order ends filled, its dust left in
-- remaining. Once past block _hf23_block (when _last_block, the replayed range's
-- last block, includes it), the open orders of the accounts its hardfork_hive
-- operations clear end cancelled there: hived cancelled them without an operation.
-- Returns the number of dust culls.
CREATE TEMP TABLE replay_orders (
  create_op_id BIGINT PRIMARY KEY, owner TEXT NOT NULL, order_id BIGINT NOT NULL,
  remaining BIGINT NOT NULL, filled BIGINT NOT NULL, end_block INT, outcome TEXT);
CREATE INDEX ON replay_orders (owner, order_id) WHERE end_block IS NULL;
CREATE TEMP TABLE _replay_ops (id BIGINT, block_num INT, trx_in_block SMALLINT, op_type_id SMALLINT, body JSONB);

CREATE FUNCTION pg_temp.replay_order_ops(_hf23_block INT, _last_block INT)
RETURNS BIGINT LANGUAGE plpgsql
AS $f$
DECLARE
  __op RECORD;
  __side RECORD;
  __trx_block INT;
  __trx_in_block INT;
  -- 'owner/order id' -> the order's last event in the current transaction.
  __trx_last JSONB;
  __key TEXT;
  __hf23_done BOOLEAN := _hf23_block IS NULL OR _hf23_block > _last_block;
  __dust_culls BIGINT := 0;
BEGIN
  TRUNCATE replay_orders;
  FOR __op IN
    SELECT o.id, o.block_num, o.trx_in_block, o.op_type_id, o.body
    FROM _replay_ops o
    WHERE o.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2(),
                           btracker_backend.op_fill_order(), btracker_backend.op_limit_order_cancel(),
                           btracker_backend.op_limit_order_cancelled())
      AND o.block_num <= _last_block
    ORDER BY o.id
  LOOP
    IF NOT __hf23_done AND __op.block_num > _hf23_block THEN
      PERFORM pg_temp.replay_hf23_close(_hf23_block);
      __hf23_done := TRUE;
    END IF;
    IF __op.trx_in_block < 0 OR __op.block_num IS DISTINCT FROM __trx_block OR __op.trx_in_block IS DISTINCT FROM __trx_in_block THEN
      __trx_block := __op.block_num;
      __trx_in_block := __op.trx_in_block;
      __trx_last := '{}';
    END IF;
    IF __op.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2()) THEN
      INSERT INTO replay_orders VALUES (__op.id, __op.body ->> 'owner', (__op.body ->> 'orderid')::BIGINT,
        (__op.body -> 'amount_to_sell' ->> 'amount')::BIGINT, 0, NULL, NULL);
      __trx_last := __trx_last || jsonb_build_object((__op.body ->> 'owner') || '/' || (__op.body ->> 'orderid'), 'create');
    ELSIF __op.op_type_id = btracker_backend.op_fill_order() THEN
      FOR __side IN
        SELECT __op.body ->> 'open_owner' AS owner, (__op.body ->> 'open_orderid')::BIGINT AS order_id,
               (__op.body -> 'open_pays' ->> 'amount')::BIGINT AS pays
        UNION ALL
        SELECT __op.body ->> 'current_owner', (__op.body ->> 'current_orderid')::BIGINT,
               (__op.body -> 'current_pays' ->> 'amount')::BIGINT
      LOOP
        CONTINUE WHEN __side.owner IS NULL;
        __trx_last := __trx_last || jsonb_build_object(__side.owner || '/' || __side.order_id, 'fill');
        UPDATE replay_orders r
           SET remaining = r.remaining - __side.pays, filled = r.filled + __side.pays,
               end_block = CASE WHEN r.remaining - __side.pays <= 0 THEN __op.block_num END,
               outcome = CASE WHEN r.remaining - __side.pays <= 0 THEN 'filled' END
         WHERE r.owner = __side.owner AND r.order_id = __side.order_id AND r.end_block IS NULL;
      END LOOP;
    ELSE
      __key := COALESCE(__op.body ->> 'seller', __op.body ->> 'owner') || '/' || (__op.body ->> 'orderid');
      IF __op.op_type_id = btracker_backend.op_limit_order_cancelled() AND __op.trx_in_block >= 0
         AND __trx_last ->> __key IS NOT DISTINCT FROM 'fill' THEN
        UPDATE replay_orders r SET end_block = __op.block_num, outcome = 'filled'
         WHERE r.owner = COALESCE(__op.body ->> 'seller', __op.body ->> 'owner')
           AND r.order_id = (__op.body ->> 'orderid')::BIGINT AND r.end_block IS NULL;
        __dust_culls := __dust_culls + 1;
      ELSE
        UPDATE replay_orders r SET end_block = __op.block_num, outcome = 'cancelled'
         WHERE r.owner = COALESCE(__op.body ->> 'seller', __op.body ->> 'owner')
           AND r.order_id = (__op.body ->> 'orderid')::BIGINT AND r.end_block IS NULL;
      END IF;
      __trx_last := __trx_last || jsonb_build_object(__key,
        CASE WHEN __op.op_type_id = btracker_backend.op_limit_order_cancel() THEN 'cancel' ELSE 'cancelled' END);
    END IF;
  END LOOP;
  IF NOT __hf23_done THEN
    PERFORM pg_temp.replay_hf23_close(_hf23_block);
  END IF;
  RETURN __dust_culls;
END
$f$;

CREATE FUNCTION pg_temp.replay_hf23_close(_hf23_block INT)
RETURNS VOID LANGUAGE sql
AS $f$
  UPDATE replay_orders r SET end_block = _hf23_block, outcome = 'cancelled'
  WHERE r.end_block IS NULL
    AND r.owner IN (SELECT o.body ->> 'account' FROM _replay_ops o
                    WHERE o.op_type_id = btracker_backend.op_hardfork_hive() AND o.block_num = _hf23_block)
$f$;

-- replay_orders against order_lifecycle, both ways; with _owner_ids, only the rows
-- of those accounts.
CREATE FUNCTION pg_temp.check_replay_equals_order_lifecycle(_when TEXT, _owner_ids INT[] = NULL)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __mismatched BIGINT;
  __example TEXT;
BEGIN
  SELECT count(*), min(d::TEXT) INTO __mismatched, __example FROM (
    WITH lifecycle AS (
      SELECT ol.create_op_id, ol.remaining, ol.filled, ol.end_block, ol.outcome FROM order_lifecycle ol
      WHERE _owner_ids IS NULL OR ol.owner_id = ANY (_owner_ids))
    (SELECT r.create_op_id, r.remaining, r.filled, r.end_block, r.outcome FROM replay_orders r
     EXCEPT
     SELECT * FROM lifecycle)
    UNION ALL
    (SELECT * FROM lifecycle
     EXCEPT
     SELECT r.create_op_id, r.remaining, r.filled, r.end_block, r.outcome FROM replay_orders r)) d;
  IF __mismatched <> 0 THEN
    RAISE EXCEPTION '%: % order_lifecycle rows differ from the replay of the order operations, e.g. %', _when, __mismatched, __example;
  END IF;
  RAISE NOTICE 'PASS: %: order_lifecycle equals the replay of the order operations (% orders: % filled, % cancelled, % open)',
    _when, (SELECT count(*) FROM replay_orders), (SELECT count(*) FROM replay_orders WHERE outcome = 'filled'),
    (SELECT count(*) FROM replay_orders WHERE outcome = 'cancelled'), (SELECT count(*) FROM replay_orders WHERE outcome IS NULL);
END
$f$;

-- The synced rows against a replay of the synced blocks' order operations.
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __head CONSTANT INT := hive.app_get_current_block_num(__schema);
  __dust_culls BIGINT;
BEGIN
  INSERT INTO _replay_ops
  SELECT o.id, o.block_num, o.trx_in_block, o.op_type_id, o.body_value AS body
  FROM operations_view o
  WHERE o.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2(),
                         btracker_backend.op_fill_order(), btracker_backend.op_limit_order_cancel(),
                         btracker_backend.op_limit_order_cancelled(), btracker_backend.op_hardfork_hive())
    AND o.block_num <= __head;
  __dust_culls := pg_temp.replay_order_ops((SELECT ah.block_num FROM hafd.applied_hardforks ah WHERE ah.hardfork_num = 23), __head);
  PERFORM pg_temp.check_replay_equals_order_lifecycle('the synced blocks');
  IF __dust_culls = 0 THEN
    RAISE NOTICE 'SKIP: the synced blocks have no dust cull, so the replay does not check them (the synthetic case below does)';
  ELSE
    RAISE NOTICE 'PASS: the replay counted % dust-culled orders as filled', __dust_culls;
  END IF;
END
$$;

BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __b3 CONSTANT INT := hive.app_get_current_block_num(__schema);
  __b2 CONSTANT INT := __b3 - 1;
  __b1 CONSTANT INT := __b3 - 2;
  -- Past any order id hived allows (uint32), so no synced order of the two accounts collides.
  __n CONSTANT BIGINT := 5000000000;
  __hive CONSTANT SMALLINT := 21;
  __hbd CONSTANT SMALLINT := 13;
  __a TEXT;
  __b TEXT;
  __a_id INT;
  __b_id INT;
  __synced_rows CONSTANT BIGINT := (SELECT count(*) FROM order_lifecycle);
  __window_before btracker_backend.order_stats := btracker_endpoints.get_order_stats(__b1::TEXT, __b3::TEXT);
  __actual JSONB;
  __expected JSONB;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  -- Two accounts without synced orders, so the per-account counts are the synthetic ones alone.
  SELECT av.name, av.id INTO __a, __a_id FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 10 LIMIT 1;
  SELECT av.name, av.id INTO __b, __b_id FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 11 LIMIT 1;
  IF __b IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts: % %', __a, __b;
  END IF;

  TRUNCATE _replay_ops;
  -- Operations past the blocks' own ones (op_pos from 10000), so their ids sort after them.
  PERFORM pg_temp.stage_ops(jsonb_build_array(
    pg_temp.op_create(__b1, 10001, __a, __n + 1, 1000, __hive),
    pg_temp.op_create(__b1, 10002, __a, __n + 2, 1000, __hbd),
    pg_temp.op_fill(__b1, 10003, __a, __n + 2, 300, __hbd),
    pg_temp.op_create(__b1, 10004, __a, __n + 6, 900, __hive),
    pg_temp.op_create(__b1, 10005, __b, __n + 4, 100, __hive, TRUE),
    pg_temp.op_cancel(__b1, 10006, __b, __n + 4, FALSE),
    pg_temp.op_cancel(__b1, 10007, __b, __n + 4, TRUE),
    pg_temp.in_trx(3, pg_temp.op_create(__b1, 10008, __a, __n + 7, 1000, __hive)),
    pg_temp.in_trx(3, pg_temp.op_fill(__b1, 10009, __a, __n + 7, 997, __hive)),
    pg_temp.in_trx(3, pg_temp.op_cancel(__b1, 10010, __a, __n + 7, TRUE)),
    pg_temp.op_create(__b1, 10011, __a, __n + 8, 1000, __hive),
    pg_temp.op_create(__b1, 10012, __b, __n + 10, 100, __hive),
    pg_temp.in_trx(4, pg_temp.op_fill(__b1, 10013, __b, __n + 10, 40, __hive)),
    pg_temp.in_trx(4, pg_temp.op_cancel(__b1, 10014, __b, __n + 10, FALSE)),
    pg_temp.in_trx(4, pg_temp.op_cancel(__b1, 10015, __b, __n + 10, TRUE)),
    pg_temp.op_create(__b1, 10016, __a, __n + 11, 500, __hive),
    pg_temp.op_create(__b1, 10017, __a, __n + 12, 400, __hive),
    pg_temp.op_create(__b2, 10001, __b, __n + 4, 200, __hive),
    pg_temp.op_create(__b2, 10002, __a, __n + 3, 500, __hive),
    pg_temp.op_create(__b2, 10003, __b, __n + 5, 100, __hbd),
    pg_temp.op_fill(__b2, 10004, __a, __n + 1, 1000, __hive),
    pg_temp.op_fill(__b2, 10005, __a, __n + 6, 200, __hive, __b, __n + 5, 40, __hbd),
    pg_temp.op_fill(__b2, 10006, __a, __n + 6, 300, __hive),
    pg_temp.op_create(__b2, 10007, __b, __n + 9, 100, __hive),
    -- One transaction: the owner cancels #11, re-creates it, and hived culls the new one.
    pg_temp.in_trx(7, pg_temp.op_cancel(__b2, 10008, __a, __n + 11, FALSE)),
    pg_temp.in_trx(7, pg_temp.op_cancel(__b2, 10009, __a, __n + 11, TRUE)),
    pg_temp.in_trx(7, pg_temp.op_create(__b2, 10010, __a, __n + 11, 1000, __hive)),
    pg_temp.in_trx(7, pg_temp.op_fill(__b2, 10011, __a, __n + 11, 998, __hive)),
    pg_temp.in_trx(7, pg_temp.op_cancel(__b2, 10012, __a, __n + 11, TRUE)),
    -- One transaction: hived culls the new #11, the owner re-creates and cancels it.
    pg_temp.in_trx(8, pg_temp.op_create(__b2, 10013, __b, __n + 11, 100, __hive)),
    pg_temp.in_trx(8, pg_temp.op_fill(__b2, 10014, __b, __n + 11, 99, __hive)),
    pg_temp.in_trx(8, pg_temp.op_cancel(__b2, 10015, __b, __n + 11, TRUE)),
    pg_temp.in_trx(8, pg_temp.op_create(__b2, 10016, __b, __n + 11, 200, __hive)),
    pg_temp.in_trx(8, pg_temp.op_cancel(__b2, 10017, __b, __n + 11, FALSE)),
    pg_temp.in_trx(8, pg_temp.op_cancel(__b2, 10018, __b, __n + 11, TRUE))));
  INSERT INTO _replay_ops SELECT id, block_num, trx_in_block, op_type_id, body_value FROM _btracker_ops_batch;
  PERFORM process_block_range_orders(__b1, __b2);
  PERFORM pg_temp.check_open_rows_equal_order_state('after a two-block range');

  PERFORM pg_temp.stage_ops(jsonb_build_array(
    pg_temp.op_cancel(__b3, 10001, __a, __n + 2, FALSE),
    pg_temp.op_cancel(__b3, 10002, __a, __n + 3, TRUE),
    pg_temp.op_fill(__b3, 10003, __b, __n + 5, 60, __hbd),
    pg_temp.op_create(__b3, 10004, __b, __n + 5, 70, __hive),
    pg_temp.op_fill(__b3, 10005, __a, __n + 6, 400, __hive),
    pg_temp.op_fill(__b3, 10006, __b, __n + 4, 50, __hive),
    pg_temp.in_trx(5, pg_temp.op_fill(__b3, 10007, __a, __n + 8, 999, __hive)),
    pg_temp.in_trx(5, pg_temp.op_cancel(__b3, 10008, __a, __n + 8, TRUE)),
    pg_temp.in_trx(6, pg_temp.op_fill(__b3, 10009, __b, __n + 9, 30, __hive)),
    pg_temp.op_cancel(__b3, 10010, __b, __n + 9, TRUE),
    -- The owner cancels #12, open from the earlier call, re-creates it, and hived culls the new one.
    pg_temp.in_trx(7, pg_temp.op_cancel(__b3, 10011, __a, __n + 12, FALSE)),
    pg_temp.in_trx(7, pg_temp.op_cancel(__b3, 10012, __a, __n + 12, TRUE)),
    pg_temp.in_trx(7, pg_temp.op_create(__b3, 10013, __a, __n + 12, 600, __hive)),
    pg_temp.in_trx(7, pg_temp.op_fill(__b3, 10014, __a, __n + 12, 597, __hive)),
    pg_temp.in_trx(7, pg_temp.op_cancel(__b3, 10015, __a, __n + 12, TRUE))));
  INSERT INTO _replay_ops SELECT id, block_num, trx_in_block, op_type_id, body_value FROM _btracker_ops_batch;
  PERFORM process_block_range_orders(__b3, __b3);
  PERFORM pg_temp.check_open_rows_equal_order_state('after the next block');
  PERFORM pg_temp.check_order_stats_rollup('after the next block');

  -- [owner, order id, created, remaining, filled, outcome, ended] per order, in creation order.
  SELECT jsonb_agg(jsonb_build_array(
           CASE ol.owner_id WHEN __a_id THEN 'a' ELSE 'b' END, ol.order_id - __n,
           ol.block_created - __b1, ol.remaining, ol.filled, ol.outcome, ol.end_block - __b1)
         ORDER BY ol.create_op_id)
  INTO __actual
  FROM order_lifecycle ol WHERE ol.owner_id IN (__a_id, __b_id);
  __expected := jsonb_build_array(
    jsonb_build_array('a', 1, 0, 0, 1000, 'filled', 1),        -- created, filled in the same range
    jsonb_build_array('a', 2, 0, 700, 300, 'cancelled', 2),    -- partially filled, then cancelled
    jsonb_build_array('a', 6, 0, 0, 900, 'filled', 2),         -- three fills over both calls
    jsonb_build_array('b', 4, 0, 100, 0, 'cancelled', 0),      -- cancel and its virtual cancelled: one outcome
    jsonb_build_array('a', 7, 0, 3, 997, 'filled', 0),         -- dust cull in the fill's transaction: filled
    jsonb_build_array('a', 8, 0, 1, 999, 'filled', 2),         -- open from the earlier call, then dust-culled
    jsonb_build_array('b', 10, 0, 60, 40, 'cancelled', 0),     -- the owner's cancel in the fill's transaction
    jsonb_build_array('a', 11, 0, 500, 0, 'cancelled', 1),     -- cancelled, then re-created in its transaction
    jsonb_build_array('a', 12, 0, 400, 0, 'cancelled', 2),     -- open from the earlier call, the same
    jsonb_build_array('b', 4, 1, 150, 50, NULL, NULL),         -- the id reused in the same range
    jsonb_build_array('a', 3, 1, 500, 0, 'cancelled', 2),      -- expired: limit_order_cancelled alone
    jsonb_build_array('b', 5, 1, 0, 100, 'filled', 2),         -- taker side of a fill, then filled
    jsonb_build_array('b', 9, 1, 70, 30, 'cancelled', 2),      -- expired in the block of a fill
    jsonb_build_array('a', 11, 1, 2, 998, 'filled', 1),        -- re-created after the owner's cancel, dust-culled
    jsonb_build_array('b', 11, 1, 1, 99, 'filled', 1),         -- dust-culled, then the id re-created and cancelled
    jsonb_build_array('b', 11, 1, 200, 0, 'cancelled', 1),     -- the re-created id, cancelled by its owner
    jsonb_build_array('b', 5, 2, 70, 0, NULL, NULL),           -- the id reused in the block that closed it
    jsonb_build_array('a', 12, 2, 3, 597, 'filled', 2));       -- re-created after the owner's cancel, dust-culled
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'order_lifecycle: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: order_lifecycle keeps each order, reused ids apart, with its outcome';

  PERFORM pg_temp.replay_order_ops(NULL, __b3);
  PERFORM pg_temp.check_replay_equals_order_lifecycle('the synthetic blocks', ARRAY[__a_id, __b_id]);

  -- Windows on the creation block; outcomes after the window still count.
  __actual := jsonb_build_array(
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__a)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__a, __b1::TEXT, __b1::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, __b2::TEXT, __b3::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, NULL, __b1::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, (__b3 + 1)::TEXT, NULL)));
  __expected := jsonb_build_array(
    jsonb_build_object('created', 10, 'filled', 6, 'cancelled', 4, 'open', 0, 'fill_rate_pct', 60),
    jsonb_build_object('created', 7, 'filled', 4, 'cancelled', 3, 'open', 0, 'fill_rate_pct', 57.14),
    jsonb_build_object('created', 6, 'filled', 2, 'cancelled', 2, 'open', 2, 'fill_rate_pct', 33.33),
    jsonb_build_object('created', 2, 'filled', 0, 'cancelled', 2, 'open', 0, 'fill_rate_pct', 0),
    jsonb_build_object('created', 0, 'filled', 0, 'cancelled', 0, 'open', 0, 'fill_rate_pct', NULL));
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/accounts/{account-name}/order-stats: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: /accounts/{account-name}/order-stats counts the orders created in the window';

  -- Network-wide: the synced orders of the window plus the eighteen synthetic ones.
  __actual := pg_temp.stats(btracker_endpoints.get_order_stats(__b1::TEXT, __b3::TEXT));
  __expected := jsonb_build_object(
    'created', __window_before.created + 18, 'filled', __window_before.filled + 8,
    'cancelled', __window_before.cancelled + 8, 'open', __window_before.open + 2,
    'fill_rate_pct', round(100.0 * (__window_before.filled + 8) / (__window_before.created + 18), 2)::FLOAT);
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/order-stats: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: /order-stats counts every account''s orders created in the window';

  IF NOT (SELECT hc.is_forking FROM hafd.contexts hc WHERE hc.name = __schema) THEN
    RAISE NOTICE 'SKIP: fork rollback: context % is not forking', __schema;
    RETURN;
  END IF;

  -- The synthetic operations' changes were recorded at the context's current block.
  PERFORM hive.context_back_from_fork(__schema, __b3 - 1);

  IF EXISTS (SELECT 1 FROM order_lifecycle WHERE owner_id IN (__a_id, __b_id))
     OR (SELECT count(*) FROM order_lifecycle) <> __synced_rows THEN
    RAISE EXCEPTION 'order_lifecycle after the fork rollback: % rows, % synced', (SELECT count(*) FROM order_lifecycle), __synced_rows;
  END IF;
  PERFORM pg_temp.check_open_rows_equal_order_state('after the fork rollback');
  PERFORM pg_temp.check_order_stats_rollup('after the fork rollback');
  IF pg_temp.stats(btracker_endpoints.get_order_stats(__b1::TEXT, __b3::TEXT)) IS DISTINCT FROM pg_temp.stats(__window_before) THEN
    RAISE EXCEPTION '/order-stats after the fork rollback: expected %, found %',
      pg_temp.stats(__window_before), pg_temp.stats(btracker_endpoints.get_order_stats(__b1::TEXT, __b3::TEXT));
  END IF;
  RAISE NOTICE 'PASS: a fork rollback reverts order_lifecycle and the order counts';
END
$$;
ROLLBACK;

-- HF23: the test HAF stops before it, so a session temp view named operations_view,
-- which process_hf_23's unqualified reference resolves to first, adds a
-- hardfork_hive operation clearing one synthetic account in a pretend HF23 block
-- (the block before the synced head); the replay gets the same operation.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __b3 CONSTANT INT := hive.app_get_current_block_num(__schema);
  __hf CONSTANT INT := __b3 - 1;
  __b1 CONSTANT INT := __b3 - 2;
  __n CONSTANT BIGINT := 5000000000;
  __hive CONSTANT SMALLINT := 21;
  __a TEXT;
  __b TEXT;
  __a_id INT;
  __b_id INT;
  __actual JSONB;
  __expected JSONB;
BEGIN
  SELECT av.name, av.id INTO __a, __a_id FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 10 LIMIT 1;
  SELECT av.name, av.id INTO __b, __b_id FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 11 LIMIT 1;
  IF __b IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts: % %', __a, __b;
  END IF;

  EXECUTE format($sql$
    CREATE TEMP VIEW operations_view AS
    SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value FROM %1$I.operations_view ov
    UNION ALL
    SELECT hafd.operation_id(%2$s, 10000), %2$s, btracker_backend.op_hardfork_hive()::SMALLINT,
           jsonb_build_object('account', %3$L, 'treasury', 'steem.dao')
  $sql$, __schema, __hf, __a);

  TRUNCATE _replay_ops;
  INSERT INTO _replay_ops VALUES (hafd.operation_id(__hf, 10000), __hf, -1, btracker_backend.op_hardfork_hive(),
    jsonb_build_object('account', __a, 'treasury', 'steem.dao'));

  PERFORM pg_temp.stage_ops(jsonb_build_array(
    pg_temp.op_create(__b1, 10001, __a, __n + 1, 1000, __hive),
    pg_temp.op_create(__b1, 10002, __a, __n + 2, 500, __hive),
    pg_temp.op_fill(__b1, 10003, __a, __n + 2, 200, __hive),
    pg_temp.op_create(__b1, 10004, __a, __n + 5, 100, __hive),
    pg_temp.op_fill(__b1, 10005, __a, __n + 5, 100, __hive),
    pg_temp.op_create(__b1, 10006, __b, __n + 1, 300, __hive),
    pg_temp.op_create(__hf, 10001, __a, __n + 3, 100, __hive),
    pg_temp.op_fill(__hf, 10002, __a, __n + 1, 100, __hive)));
  INSERT INTO _replay_ops SELECT id, block_num, trx_in_block, op_type_id, body_value FROM _btracker_ops_batch;
  PERFORM process_block_range_orders(__b1, __hf);
  PERFORM btracker_backend.process_hf_23(__hf);

  PERFORM pg_temp.stage_ops(jsonb_build_array(
    pg_temp.op_create(__b3, 10001, __a, __n + 1, 50, __hive)));
  INSERT INTO _replay_ops SELECT id, block_num, trx_in_block, op_type_id, body_value FROM _btracker_ops_batch;
  PERFORM process_block_range_orders(__b3, __b3);
  PERFORM pg_temp.check_open_rows_equal_order_state('after HF23');
  PERFORM pg_temp.check_order_stats_rollup('after HF23');

  SELECT jsonb_agg(jsonb_build_array(
           CASE ol.owner_id WHEN __a_id THEN 'a' ELSE 'b' END, ol.order_id - __n,
           ol.block_created - __b1, ol.remaining, ol.filled, ol.outcome, ol.end_block - __b1)
         ORDER BY ol.create_op_id)
  INTO __actual
  FROM order_lifecycle ol WHERE ol.owner_id IN (__a_id, __b_id);
  __expected := jsonb_build_array(
    jsonb_build_array('a', 1, 0, 900, 100, 'cancelled', 1),    -- open at HF23: cancelled there
    jsonb_build_array('a', 2, 0, 300, 200, 'cancelled', 1),    -- partially filled, open at HF23
    jsonb_build_array('a', 5, 0, 0, 100, 'filled', 0),         -- filled before HF23: unchanged
    jsonb_build_array('b', 1, 0, 300, 0, NULL, NULL),          -- an account HF23 does not clear
    jsonb_build_array('a', 3, 1, 100, 0, 'cancelled', 1),      -- created in the HF23 block
    jsonb_build_array('a', 1, 2, 50, 0, NULL, NULL));          -- the id reused after HF23
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'order_lifecycle after HF23: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: HF23 closes the cleared account''s open orders as cancelled';

  PERFORM pg_temp.replay_order_ops(__hf, __b3);
  PERFORM pg_temp.check_replay_equals_order_lifecycle('across HF23', ARRAY[__a_id, __b_id]);
  PERFORM pg_temp.replay_order_ops(__hf, __hf);
  IF (SELECT count(*) FROM replay_orders WHERE outcome = 'cancelled' AND end_block = __hf) <> 3 THEN
    RAISE EXCEPTION 'replay ending at the HF23 block: expected its 3 open orders of % cancelled there', __a;
  END IF;
  RAISE NOTICE 'PASS: the replay closes the cleared account''s open orders at HF23, also when it ends there';
END
$$;
ROLLBACK;

-- An install upgraded from a version with order_lifecycle but without its rollups:
-- the rollups dropped, db/btracker_app.sql run again as scripts/install_app.sh
-- runs it, then a block that fills one order created before the upgrade and
-- cancels another.
BEGIN;
DROP TABLE order_stats_by_block, order_stats_by_block_group;
SET search_path TO :"schema";
\ir ../../db/btracker_app.sql
SET search_path TO :"schema", public;
DO
$$
DECLARE
  __b3 CONSTANT INT := hive.app_get_current_block_num(current_setting('btracker_test.schema'));
  __filled RECORD;
  __cancelled RECORD;
  __filled_before btracker_backend.order_stats;
  __cancelled_before btracker_backend.order_stats;
  __actual JSONB;
  __expected JSONB;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM order_stats_by_block) THEN
    RAISE EXCEPTION 'reinstalling over order_lifecycle (% rows) left order_stats_by_block empty', (SELECT count(*) FROM order_lifecycle);
  END IF;
  PERFORM pg_temp.check_order_stats_rollup('after reinstalling over order_lifecycle without the rollups');

  SELECT ol.*, av.name INTO __filled FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.end_block IS NULL ORDER BY ol.create_op_id LIMIT 1;
  SELECT ol.*, av.name INTO __cancelled FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.end_block IS NULL ORDER BY ol.create_op_id OFFSET 1 LIMIT 1;
  IF __cancelled.create_op_id IS NULL THEN
    RAISE EXCEPTION 'expected two open synced orders, found %', (SELECT count(*) FROM order_lifecycle WHERE end_block IS NULL);
  END IF;
  __filled_before := btracker_endpoints.get_order_stats(__filled.block_created::TEXT, __filled.block_created::TEXT);
  __cancelled_before := btracker_endpoints.get_order_stats(__cancelled.block_created::TEXT, __cancelled.block_created::TEXT);

  PERFORM pg_temp.stage_ops(jsonb_build_array(
    pg_temp.op_fill(__b3, 10001, __filled.name, __filled.order_id, __filled.remaining, __filled.nai),
    pg_temp.op_cancel(__b3, 10002, __cancelled.name, __cancelled.order_id, TRUE)));
  PERFORM process_block_range_orders(__b3, __b3);
  PERFORM pg_temp.check_open_rows_equal_order_state('after a block past the upgrade');
  PERFORM pg_temp.check_order_stats_rollup('after a block past the upgrade');

  -- Each order's creation block keeps it created, now closed instead of open.
  __actual := jsonb_build_array(
    pg_temp.stats(btracker_endpoints.get_order_stats(__filled.block_created::TEXT, __filled.block_created::TEXT)),
    pg_temp.stats(btracker_endpoints.get_order_stats(__cancelled.block_created::TEXT, __cancelled.block_created::TEXT)));
  IF __filled.block_created = __cancelled.block_created THEN
    __expected := jsonb_build_object(
      'created', __filled_before.created, 'filled', __filled_before.filled + 1,
      'cancelled', __filled_before.cancelled + 1, 'open', __filled_before.open - 2,
      'fill_rate_pct', round(100.0 * (__filled_before.filled + 1) / __filled_before.created, 2)::FLOAT);
    __expected := jsonb_build_array(__expected, __expected);
  ELSE
    __expected := jsonb_build_array(
      jsonb_build_object(
        'created', __filled_before.created, 'filled', __filled_before.filled + 1,
        'cancelled', __filled_before.cancelled, 'open', __filled_before.open - 1,
        'fill_rate_pct', round(100.0 * (__filled_before.filled + 1) / __filled_before.created, 2)::FLOAT),
      jsonb_build_object(
        'created', __cancelled_before.created, 'filled', __cancelled_before.filled,
        'cancelled', __cancelled_before.cancelled + 1, 'open', __cancelled_before.open - 1,
        'fill_rate_pct', round(100.0 * __cancelled_before.filled / __cancelled_before.created, 2)::FLOAT));
  END IF;
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/order-stats over the closed orders'' creation blocks: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: orders created before the upgrade and closed after it count as created and closed: %', __actual;
END
$$;
ROLLBACK;

-- The same upgrade over reversible blocks: three blocks past the synced head, past
-- the context's irreversible block (lowered to that head), processed one at a time
-- as live blocks (each change recorded at its block), creating orders and closing
-- them and two synced ones; then the rollups dropped and db/btracker_app.sql run
-- again. A fork back to the first of them, then to the irreversible block, and a
-- different chain processed after it.
CREATE FUNCTION pg_temp.process_live_block(_block INT, _ops JSONB)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  UPDATE hafd.contexts SET current_block_num = _block WHERE name = current_setting('btracker_test.schema');
  PERFORM pg_temp.stage_ops(_ops);
  PERFORM process_block_range_orders(_block, _block);
END
$f$;

SELECT hc.is_forking AS context_is_forking FROM hafd.contexts hc WHERE hc.name = :'schema' \gset
\if :context_is_forking
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __h CONSTANT INT := hive.app_get_current_block_num(__schema);
  __n CONSTANT BIGINT := 5000000000;
  __hive CONSTANT SMALLINT := 21;
  __a TEXT;
  __b TEXT;
  __o1 RECORD;
  __o2 RECORD;
BEGIN
  SELECT av.name INTO __a FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 10 LIMIT 1;
  SELECT av.name INTO __b FROM hive.accounts_view av
  WHERE NOT EXISTS (SELECT 1 FROM order_lifecycle ol WHERE ol.owner_id = av.id) ORDER BY av.id OFFSET 11 LIMIT 1;
  SELECT ol.*, av.name INTO __o1 FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.end_block IS NULL ORDER BY ol.create_op_id LIMIT 1;
  SELECT ol.*, av.name INTO __o2 FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.end_block IS NULL ORDER BY ol.create_op_id OFFSET 1 LIMIT 1;
  IF __b IS NULL OR __o2.create_op_id IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts (% %) or fewer than two open synced orders', __a, __b;
  END IF;
  PERFORM set_config('btracker_test.h', __h::TEXT, false);
  PERFORM set_config('btracker_test.a', __a, false);
  PERFORM set_config('btracker_test.b', __b, false);
  PERFORM set_config('btracker_test.o1', __o1.create_op_id::TEXT, false);
  PERFORM set_config('btracker_test.o2', __o2.create_op_id::TEXT, false);

  UPDATE hafd.contexts SET irreversible_block = __h WHERE name = __schema;

  PERFORM pg_temp.process_live_block(__h + 1, jsonb_build_array(
    pg_temp.op_create(__h + 1, 10001, __a, __n + 1, 1000, __hive),
    pg_temp.op_create(__h + 1, 10002, __a, __n + 2, 500, __hive),
    pg_temp.op_create(__h + 1, 10003, __b, __n + 1, 100, __hive),
    pg_temp.op_fill(__h + 1, 10004, __o1.name, __o1.order_id, __o1.remaining, __o1.nai)));
  PERFORM pg_temp.process_live_block(__h + 2, jsonb_build_array(
    pg_temp.op_fill(__h + 2, 10001, __a, __n + 1, 1000, __hive),
    pg_temp.op_cancel(__h + 2, 10002, __b, __n + 1, FALSE),
    pg_temp.op_cancel(__h + 2, 10003, __b, __n + 1, TRUE),
    pg_temp.op_create(__h + 2, 10004, __b, __n + 2, 100, __hive)));
  PERFORM pg_temp.process_live_block(__h + 3, jsonb_build_array(
    pg_temp.op_cancel(__h + 3, 10001, __o2.name, __o2.order_id, TRUE),
    pg_temp.op_fill(__h + 3, 10002, __a, __n + 2, 200, __hive)));
  PERFORM pg_temp.check_order_stats_rollup('after three reversible blocks');
END
$$;
-- One table per statement: HAF unregisters only one table a DROP TABLE drops, and
-- would keep the other's registration and the shadow rows of these blocks.
DROP TABLE order_stats_by_block;
DROP TABLE order_stats_by_block_group;
SET search_path TO :"schema";
\ir ../../db/btracker_app.sql
SET search_path TO :"schema", public;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __h CONSTANT INT := current_setting('btracker_test.h')::INT;
  __a CONSTANT TEXT := current_setting('btracker_test.a');
  __b CONSTANT TEXT := current_setting('btracker_test.b');
  __n CONSTANT BIGINT := 5000000000;
  __hive CONSTANT SMALLINT := 21;
  __o1 RECORD;
  __o2 RECORD;
  __actual JSONB;
BEGIN
  IF hive.app_get_current_block_num(__schema) <> __h + 3 THEN
    RAISE EXCEPTION 'seeding over reversible blocks left the context at block %, expected %', hive.app_get_current_block_num(__schema), __h + 3;
  END IF;
  PERFORM pg_temp.check_order_stats_rollup('after seeding over reversible blocks');

  -- Keeps the first reversible block: its creations and o1's fill stay counted,
  -- the later blocks' changes are gone from order_lifecycle and the rollups alike.
  PERFORM hive.context_back_from_fork(__schema, __h + 1);
  SELECT jsonb_build_array(
           (SELECT count(*) FROM order_lifecycle WHERE block_created > __h),
           (SELECT count(*) FROM order_lifecycle WHERE end_block > __h),
           (SELECT outcome FROM order_lifecycle WHERE create_op_id = current_setting('btracker_test.o1')::BIGINT))
  INTO __actual;
  IF __actual IS DISTINCT FROM jsonb_build_array(3, 1, 'filled') THEN
    RAISE EXCEPTION 'order_lifecycle after a fork back to the first reversible block: expected [3, 1, "filled"] (created, closed past the irreversible block, o1), found %', __actual;
  END IF;
  PERFORM pg_temp.check_open_rows_equal_order_state('after a fork back to the first reversible block');
  PERFORM pg_temp.check_order_stats_rollup('after a fork back to the first reversible block');

  -- Leaves the irreversible history alone, what the seed inserted unrecorded.
  PERFORM hive.context_back_from_fork(__schema, __h);
  IF EXISTS (SELECT 1 FROM order_lifecycle WHERE block_created > __h OR end_block > __h) THEN
    RAISE EXCEPTION 'order_lifecycle after a fork back to the irreversible block still holds rows past block %', __h;
  END IF;
  PERFORM pg_temp.check_open_rows_equal_order_state('after a fork back to the irreversible block');
  PERFORM pg_temp.check_order_stats_rollup('after a fork back to the irreversible block');

  SELECT ol.*, av.name INTO __o1 FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.create_op_id = current_setting('btracker_test.o1')::BIGINT;
  SELECT ol.*, av.name INTO __o2 FROM order_lifecycle ol JOIN hive.accounts_view av ON av.id = ol.owner_id
  WHERE ol.create_op_id = current_setting('btracker_test.o2')::BIGINT;
  PERFORM pg_temp.process_live_block(__h + 1, jsonb_build_array(
    pg_temp.op_create(__h + 1, 10001, __a, __n + 1, 700, __hive),
    pg_temp.op_cancel(__h + 1, 10002, __o1.name, __o1.order_id, TRUE)));
  PERFORM pg_temp.process_live_block(__h + 2, jsonb_build_array(
    pg_temp.op_fill(__h + 2, 10001, __o2.name, __o2.order_id, __o2.remaining, __o2.nai),
    pg_temp.op_create(__h + 2, 10002, __b, __n + 1, 300, __hive),
    pg_temp.op_fill(__h + 2, 10003, __b, __n + 1, 100, __hive)));
  SELECT jsonb_build_array(
           (SELECT count(*) FROM order_lifecycle WHERE block_created > __h),
           (SELECT outcome FROM order_lifecycle WHERE create_op_id = __o1.create_op_id),
           (SELECT outcome FROM order_lifecycle WHERE create_op_id = __o2.create_op_id))
  INTO __actual;
  IF __actual IS DISTINCT FROM jsonb_build_array(2, 'cancelled', 'filled') THEN
    RAISE EXCEPTION 'order_lifecycle after a different chain: expected [2, "cancelled", "filled"] (created, o1, o2), found %', __actual;
  END IF;
  PERFORM pg_temp.check_open_rows_equal_order_state('after a different chain past the fork');
  PERFORM pg_temp.check_order_stats_rollup('after a different chain past the fork');
  RAISE NOTICE 'PASS: seeding over reversible blocks counts them so that a fork reverts them and a different chain counts its own';
END
$$;
ROLLBACK;
\else
\echo 'SKIP: seeding over reversible blocks: the context is not forking'
\endif
