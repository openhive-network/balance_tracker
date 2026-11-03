/*
 * hf_23_orders.sql
 * ================
 * The open limit orders of the accounts cleared at HF23, which hived cancelled
 * without a limit_order_cancelled operation: process_hf_23 deletes a cleared
 * account's order_state rows and closes its open order_lifecycle rows as
 * cancelled at the HF23 block with their remaining kept, also in the counts of
 * their creation blocks (order_stats_by_block, order_stats_by_block_group), and
 * leaves an order it closed earlier and every order of an account it does not
 * clear unchanged.
 *
 * The test HAF stops at block 5,000,000, before HF23: a session temp view named
 * operations_view, which the application's unqualified references resolve to
 * first, adds to the context's view a hardfork_hive operation clearing a real
 * account in a pretend HF23 block H (10 blocks below the head). That account and
 * one it does not clear each get two open orders and one closed order.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. The check runs in a transaction that is rolled
 * back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

CREATE TEMP TABLE _hf23_fixture AS
SELECT
  (SELECT av.id FROM accounts_view av ORDER BY av.id OFFSET 12 LIMIT 1) AS account,
  (SELECT av.name FROM accounts_view av ORDER BY av.id OFFSET 12 LIMIT 1) AS name,
  (SELECT av.id FROM accounts_view av ORDER BY av.id OFFSET 13 LIMIT 1) AS other_account,
  hive.app_get_current_block_num(:'schema') - 10 AS hf_block;

DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
BEGIN
  IF (SELECT count(*) FROM _hf23_fixture WHERE account IS NOT NULL AND other_account IS NOT NULL) <> 1 THEN
    RAISE EXCEPTION 'no fixture accounts';
  END IF;

  EXECUTE format($sql$
    CREATE TEMP VIEW operations_view AS
    SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value FROM %1$I.operations_view ov
    UNION ALL
    SELECT hafd.operation_id(f.hf_block, 10000), f.hf_block, btracker_backend.op_hardfork_hive()::SMALLINT,
           jsonb_build_object('account', f.name, 'treasury', 'steem.dao')
    FROM _hf23_fixture f
  $sql$, __schema);
END
$$;

BEGIN;
DO
$$
DECLARE
  __f RECORD;
  __changes TEXT;
  __expected TEXT;
  __stats_before btracker_backend.order_stats;
  __stats_after btracker_backend.order_stats;
  __group_cancelled_before BIGINT;
BEGIN
  SELECT * INTO __f FROM _hf23_fixture;

  DELETE FROM order_state WHERE owner_id IN (__f.account, __f.other_account);
  DELETE FROM order_lifecycle WHERE owner_id IN (__f.account, __f.other_account);

  -- (owner, order_id, create block offset from H, remaining, filled, end block offset or NULL while open, outcome)
  INSERT INTO order_lifecycle (create_op_id, order_id, remaining, filled, owner_id, block_created, end_block, nai, outcome)
  SELECT hafd.operation_id(__f.hf_block + o.created, 10001 + o.n), o.order_id, o.remaining, o.filled, o.owner,
         __f.hf_block + o.created, __f.hf_block + o.ended, 21::SMALLINT, o.outcome
  FROM (VALUES
    (0, __f.account,       1::BIGINT, -5, 700::BIGINT, 300::BIGINT, NULL::INT, NULL::TEXT),
    (1, __f.account,       2,          0, 1000,        0,           NULL,      NULL),
    (2, __f.account,       3,         -6, 0,           500,         -3,        'filled'),
    (3, __f.other_account, 1,         -5, 400,         100,         NULL,      NULL),
    (4, __f.other_account, 2,          0, 900,         0,           NULL,      NULL),
    (5, __f.other_account, 3,         -6, 200,         0,           -2,        'cancelled')
  ) AS o(n, owner, order_id, created, remaining, filled, ended, outcome);

  INSERT INTO order_state (owner_id, order_id, nai, remaining, block_created)
  SELECT owner_id, order_id, nai, remaining, block_created FROM order_lifecycle
  WHERE owner_id IN (__f.account, __f.other_account) AND end_block IS NULL;

  -- The fixture orders in the counts, as block processing would have added them.
  INSERT INTO order_stats_by_block AS s (block_created, created, filled, cancelled)
  SELECT block_created, count(*), count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
  FROM order_lifecycle WHERE owner_id IN (__f.account, __f.other_account) GROUP BY block_created
  ON CONFLICT (block_created) DO UPDATE
    SET created = s.created + EXCLUDED.created, filled = s.filled + EXCLUDED.filled, cancelled = s.cancelled + EXCLUDED.cancelled;
  INSERT INTO order_stats_by_block_group AS s (block_group, created, filled, cancelled)
  SELECT block_created / btracker_backend.order_stats_group_blocks(), count(*),
         count(*) FILTER (WHERE outcome = 'filled'), count(*) FILTER (WHERE outcome = 'cancelled')
  FROM order_lifecycle WHERE owner_id IN (__f.account, __f.other_account) GROUP BY 1
  ON CONFLICT (block_group) DO UPDATE
    SET created = s.created + EXCLUDED.created, filled = s.filled + EXCLUDED.filled, cancelled = s.cancelled + EXCLUDED.cancelled;
  __stats_before := btracker_endpoints.get_order_stats((__f.hf_block - 6)::TEXT, __f.hf_block::TEXT);
  __group_cancelled_before := (SELECT sum(cancelled) FROM order_stats_by_block_group);

  CREATE TEMP TABLE _order_state_before ON COMMIT DROP AS SELECT * FROM order_state;
  CREATE TEMP TABLE _order_lifecycle_before ON COMMIT DROP AS SELECT * FROM order_lifecycle;

  PERFORM btracker_backend.process_hf_23(__f.hf_block);
  __stats_after := btracker_endpoints.get_order_stats((__f.hf_block - 6)::TEXT, __f.hf_block::TEXT);

  SELECT string_agg(format('%s (%s, %s)', c.change, c.owner_id, c.order_id), ', ' ORDER BY c.change, c.owner_id, c.order_id)
  INTO __changes
  FROM (
    (SELECT 'removed' AS change, t.owner_id, t.order_id FROM (SELECT * FROM _order_state_before EXCEPT ALL SELECT * FROM order_state) t)
    UNION ALL
    (SELECT 'added', t.owner_id, t.order_id FROM (SELECT * FROM order_state EXCEPT ALL SELECT * FROM _order_state_before) t)
  ) c;
  __expected := format('removed (%1$s, 1), removed (%1$s, 2)', __f.account);
  IF __changes IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'process_hf_23 order_state: expected %, found %', __expected, __changes;
  END IF;
  RAISE NOTICE 'PASS: process_hf_23 deletes the cleared account''s open orders from order_state and leaves every other row unchanged';

  SELECT string_agg(format('%s (%s, %s, %s, %s, %s, %s)', c.change, c.owner_id, c.order_id, c.remaining, c.filled, c.end_block, c.outcome),
                    ', ' ORDER BY c.owner_id, c.order_id, c.change)
  INTO __changes
  FROM (
    (SELECT 'was' AS change, t.* FROM (SELECT * FROM _order_lifecycle_before EXCEPT ALL SELECT * FROM order_lifecycle) t)
    UNION ALL
    (SELECT 'now', t.* FROM (SELECT * FROM order_lifecycle EXCEPT ALL SELECT * FROM _order_lifecycle_before) t)
  ) c;
  __expected := format('now (%1$s, 1, 700, 300, %2$s, cancelled), was (%1$s, 1, 700, 300, , ), '
                       'now (%1$s, 2, 1000, 0, %2$s, cancelled), was (%1$s, 2, 1000, 0, , )', __f.account, __f.hf_block);
  IF __changes IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'process_hf_23 order_lifecycle: expected %, found %', __expected, __changes;
  END IF;
  RAISE NOTICE 'PASS: process_hf_23 closes the cleared account''s open orders as cancelled at HF23 with their remaining kept, and leaves every other row unchanged';

  IF EXISTS (
    (SELECT owner_id, order_id, nai, remaining, block_created FROM order_lifecycle WHERE end_block IS NULL
     EXCEPT ALL SELECT owner_id, order_id, nai, remaining, block_created FROM order_state)
    UNION ALL
    (SELECT owner_id, order_id, nai, remaining, block_created FROM order_state
     EXCEPT ALL SELECT owner_id, order_id, nai, remaining, block_created FROM order_lifecycle WHERE end_block IS NULL)
  ) THEN
    RAISE EXCEPTION 'process_hf_23: the open order_lifecycle rows differ from order_state';
  END IF;
  RAISE NOTICE 'PASS: after process_hf_23 the open order_lifecycle rows equal order_state';

  IF (__stats_after.created, __stats_after.filled, __stats_after.cancelled, __stats_after.open)
     IS DISTINCT FROM (__stats_before.created, __stats_before.filled, __stats_before.cancelled + 2, __stats_before.open - 2) THEN
    RAISE EXCEPTION 'process_hf_23 /order-stats over the fixture blocks: expected the two closed orders moved from open to cancelled in %, found %',
      to_jsonb(__stats_before), to_jsonb(__stats_after);
  END IF;
  IF (SELECT sum(cancelled) FROM order_stats_by_block_group) IS DISTINCT FROM __group_cancelled_before + 2 THEN
    RAISE EXCEPTION 'process_hf_23 order_stats_by_block_group: expected % cancelled, found %',
      __group_cancelled_before + 2, (SELECT sum(cancelled) FROM order_stats_by_block_group);
  END IF;
  RAISE NOTICE 'PASS: process_hf_23 counts the orders it closes as cancelled in /order-stats and the block groups';
END
$$;
ROLLBACK;
