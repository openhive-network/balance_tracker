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
 *     synced order operations one at a time;
 *   - synthetic order operations in the three blocks before the synced head: a
 *     two-block range processed as massive sync does, then the next block as a
 *     live block. Covers create -> fill, create -> partial fill -> cancel,
 *     expiry (a virtual limit_order_cancelled only, also after a fill in the
 *     same block), several fills across both calls, a user cancel followed by
 *     its virtual limit_order_cancelled (also in the transaction of a fill), a
 *     dust cull (hived's limit_order_cancelled after a fill in its transaction,
 *     an order hived counts as filled) of an order created in the range and of
 *     one open from the earlier call, and an order id reused within a range and
 *     across the two. Every row, order_state
 *     against the open rows, and the endpoints' windows are checked;
 *   - after HAF rolls the context back (hive.context_back_from_fork) the
 *     synthetic rows are gone and the synced ones are back (skipped when the
 *     context is not forking).
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
END
$$;

-- The synced rows against a replay of the synced blocks' order operations one by
-- one, in operation order, reading the operation bodies directly. A virtual
-- limit_order_cancelled in a transaction, after a fill of the order in that
-- transaction and with no limit_order_cancel of it there, is hived's dust cull
-- of a fully filled order: the order ends filled, its dust left in remaining.
CREATE TEMP TABLE replay_orders (
  create_op_id BIGINT PRIMARY KEY, owner TEXT NOT NULL, order_id BIGINT NOT NULL,
  remaining BIGINT NOT NULL, filled BIGINT NOT NULL, end_block INT, outcome TEXT);
CREATE INDEX ON replay_orders (owner, order_id) WHERE end_block IS NULL;

DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __op RECORD;
  __side RECORD;
  __trx_block INT;
  __trx_in_block INT;
  __trx_filled TEXT[];
  __trx_user_cancelled TEXT[];
  __key TEXT;
  __dust_culls BIGINT := 0;
  __mismatched BIGINT;
  __example TEXT;
BEGIN
  FOR __op IN
    SELECT o.id, o.block_num, o.trx_in_block, o.op_type_id, o.body_value AS body
    FROM operations_view o
    WHERE o.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2(),
                           btracker_backend.op_fill_order(), btracker_backend.op_limit_order_cancel(),
                           btracker_backend.op_limit_order_cancelled())
      AND o.block_num <= hive.app_get_current_block_num(__schema)
    ORDER BY o.id
  LOOP
    IF __op.trx_in_block < 0 OR __op.block_num IS DISTINCT FROM __trx_block OR __op.trx_in_block IS DISTINCT FROM __trx_in_block THEN
      __trx_block := __op.block_num;
      __trx_in_block := __op.trx_in_block;
      __trx_filled := '{}';
      __trx_user_cancelled := '{}';
    END IF;
    IF __op.op_type_id IN (btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2()) THEN
      INSERT INTO replay_orders VALUES (__op.id, __op.body ->> 'owner', (__op.body ->> 'orderid')::BIGINT,
        (__op.body -> 'amount_to_sell' ->> 'amount')::BIGINT, 0, NULL, NULL);
    ELSIF __op.op_type_id = btracker_backend.op_fill_order() THEN
      FOR __side IN
        SELECT __op.body ->> 'open_owner' AS owner, (__op.body ->> 'open_orderid')::BIGINT AS order_id,
               (__op.body -> 'open_pays' ->> 'amount')::BIGINT AS pays
        UNION ALL
        SELECT __op.body ->> 'current_owner', (__op.body ->> 'current_orderid')::BIGINT,
               (__op.body -> 'current_pays' ->> 'amount')::BIGINT
      LOOP
        __trx_filled := __trx_filled || (__side.owner || '/' || __side.order_id);
        UPDATE replay_orders r
           SET remaining = r.remaining - __side.pays, filled = r.filled + __side.pays,
               end_block = CASE WHEN r.remaining - __side.pays <= 0 THEN __op.block_num END,
               outcome = CASE WHEN r.remaining - __side.pays <= 0 THEN 'filled' END
         WHERE r.owner = __side.owner AND r.order_id = __side.order_id AND r.end_block IS NULL;
      END LOOP;
    ELSE
      __key := COALESCE(__op.body ->> 'seller', __op.body ->> 'owner') || '/' || (__op.body ->> 'orderid');
      IF __op.op_type_id = btracker_backend.op_limit_order_cancel() THEN
        __trx_user_cancelled := __trx_user_cancelled || __key;
      END IF;
      IF __op.op_type_id = btracker_backend.op_limit_order_cancelled() AND __op.trx_in_block >= 0
         AND __key = ANY (__trx_filled) AND NOT __key = ANY (__trx_user_cancelled) THEN
        UPDATE replay_orders r SET end_block = __op.block_num, outcome = 'filled'
         WHERE r.owner = COALESCE(__op.body ->> 'seller', __op.body ->> 'owner')
           AND r.order_id = (__op.body ->> 'orderid')::BIGINT AND r.end_block IS NULL;
        __dust_culls := __dust_culls + 1;
      ELSE
        UPDATE replay_orders r SET end_block = __op.block_num, outcome = 'cancelled'
         WHERE r.owner = COALESCE(__op.body ->> 'seller', __op.body ->> 'owner')
           AND r.order_id = (__op.body ->> 'orderid')::BIGINT AND r.end_block IS NULL;
      END IF;
    END IF;
  END LOOP;

  SELECT count(*), min(d::TEXT) INTO __mismatched, __example FROM (
    (SELECT r.create_op_id, r.remaining, r.filled, r.end_block, r.outcome FROM replay_orders r
     EXCEPT
     SELECT ol.create_op_id, ol.remaining, ol.filled, ol.end_block, ol.outcome FROM order_lifecycle ol)
    UNION ALL
    (SELECT ol.create_op_id, ol.remaining, ol.filled, ol.end_block, ol.outcome FROM order_lifecycle ol
     EXCEPT
     SELECT r.create_op_id, r.remaining, r.filled, r.end_block, r.outcome FROM replay_orders r)) d;
  IF __mismatched <> 0 THEN
    RAISE EXCEPTION '% order_lifecycle rows differ from the replay of the order operations, e.g. %', __mismatched, __example;
  END IF;
  RAISE NOTICE 'PASS: order_lifecycle equals the replay of the synced order operations (% orders: % filled, % cancelled, % open)',
    (SELECT count(*) FROM replay_orders), (SELECT count(*) FROM replay_orders WHERE outcome = 'filled'),
    (SELECT count(*) FROM replay_orders WHERE outcome = 'cancelled'), (SELECT count(*) FROM replay_orders WHERE outcome IS NULL);
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
    pg_temp.op_create(__b2, 10001, __b, __n + 4, 200, __hive),
    pg_temp.op_create(__b2, 10002, __a, __n + 3, 500, __hive),
    pg_temp.op_create(__b2, 10003, __b, __n + 5, 100, __hbd),
    pg_temp.op_fill(__b2, 10004, __a, __n + 1, 1000, __hive),
    pg_temp.op_fill(__b2, 10005, __a, __n + 6, 200, __hive, __b, __n + 5, 40, __hbd),
    pg_temp.op_fill(__b2, 10006, __a, __n + 6, 300, __hive),
    pg_temp.op_create(__b2, 10007, __b, __n + 9, 100, __hive)));
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
    pg_temp.op_cancel(__b3, 10010, __b, __n + 9, TRUE)));
  PERFORM process_block_range_orders(__b3, __b3);
  PERFORM pg_temp.check_open_rows_equal_order_state('after the next block');

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
    jsonb_build_array('b', 4, 1, 150, 50, NULL, NULL),         -- the id reused in the same range
    jsonb_build_array('a', 3, 1, 500, 0, 'cancelled', 2),      -- expired: limit_order_cancelled alone
    jsonb_build_array('b', 5, 1, 0, 100, 'filled', 2),         -- taker side of a fill, then filled
    jsonb_build_array('b', 9, 1, 70, 30, 'cancelled', 2),      -- expired in the block of a fill
    jsonb_build_array('b', 5, 2, 70, 0, NULL, NULL));          -- the id reused in the block that closed it
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'order_lifecycle: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: order_lifecycle keeps each order, reused ids apart, with its outcome';

  -- Windows on the creation block; outcomes after the window still count.
  __actual := jsonb_build_array(
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__a)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__a, __b1::TEXT, __b1::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, __b2::TEXT, __b3::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, NULL, __b1::TEXT)),
    pg_temp.stats(btracker_endpoints.get_account_order_stats(__b, (__b3 + 1)::TEXT, NULL)));
  __expected := jsonb_build_array(
    jsonb_build_object('created', 6, 'filled', 4, 'cancelled', 2, 'open', 0, 'fill_rate_pct', 66.67),
    jsonb_build_object('created', 5, 'filled', 4, 'cancelled', 1, 'open', 0, 'fill_rate_pct', 80),
    jsonb_build_object('created', 4, 'filled', 1, 'cancelled', 1, 'open', 2, 'fill_rate_pct', 25),
    jsonb_build_object('created', 2, 'filled', 0, 'cancelled', 2, 'open', 0, 'fill_rate_pct', 0),
    jsonb_build_object('created', 0, 'filled', 0, 'cancelled', 0, 'open', 0, 'fill_rate_pct', NULL));
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/accounts/{account-name}/order-stats: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: /accounts/{account-name}/order-stats counts the orders created in the window';

  -- Network-wide: the synced orders of the window plus the twelve synthetic ones.
  __actual := pg_temp.stats(btracker_endpoints.get_order_stats(__b1::TEXT, __b3::TEXT));
  __expected := jsonb_build_object(
    'created', __window_before.created + 12, 'filled', __window_before.filled + 5,
    'cancelled', __window_before.cancelled + 5, 'open', __window_before.open + 2,
    'fill_rate_pct', round(100.0 * (__window_before.filled + 5) / (__window_before.created + 12), 2)::FLOAT);
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
  RAISE NOTICE 'PASS: a fork rollback reverts order_lifecycle';
END
$$;
ROLLBACK;
