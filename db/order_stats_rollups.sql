SET ROLE btracker_owner;

/**
 * Order count rollups
 * ===================
 * order_stats_by_block counts the orders created in each block and, of them,
 * those that ended filled and cancelled; order_stats_by_block_group holds the
 * same sums per group of btracker_backend.order_stats_group_blocks() blocks
 * (block_group = block_created / that). process_block_range_orders adds to both
 * in the statement that writes order_lifecycle, an outcome to the creation
 * block's row when it lands, so for any creation-block window they count what
 * order_lifecycle does. The network-wide statistics read whole groups and, at
 * the window's edges, at most a group's worth of blocks. Each is seeded from
 * order_lifecycle when it is created, so an install upgraded from a version
 * that had order_lifecycle but not the rollups counts its earlier orders too:
 * the additive upserts of later blocks need that base, or an outcome landing on
 * an earlier order would count it closed but never created.
 *
 * Rows inserted before hive.app_register_table get no shadow rows, so a fork
 * never reverts them: the seed inserts only the irreversible history that way,
 * and adds the reversible blocks' part after registering, recorded per block
 * (see the seed). After a fork HAF reverts the registered tables to a block at
 * or above the irreversible one and processing resumes after it, so the blocks
 * whose part a fork reverted are processed again and add it back, and no block
 * up to the irreversible one is processed twice.
 *
 * Every writer of order_lifecycle must maintain both rollups in the same
 * statement: today process_block_range_orders and btracker_backend.process_hf_23.
 *
 * scripts/install_app.sh runs this file in one transaction with
 * db/process_block_range_orders.sql, once every function that one calls is
 * installed: with the application running during an upgrade, the first batch
 * that can see the seeded rollups is also the first to run the version that
 * maintains them, and none commits blocks the seed did not count without
 * adding them. Once both exist, a re-run changes nothing and takes no lock.
 */
DO $$
DECLARE
  __schema_name VARCHAR;
  __irreversible INT;
  __current_block INT;
  __event_block INT;
  __reversible RECORD;
  __seed_by_block BOOLEAN;
  __seed_by_block_group BOOLEAN;
  __group_blocks CONSTANT INT := btracker_backend.order_stats_group_blocks();
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;

  __seed_by_block := to_regclass(format('%I.order_stats_by_block', __schema_name)) IS NULL;
  __seed_by_block_group := to_regclass(format('%I.order_stats_by_block_group', __schema_name)) IS NULL;
  IF NOT (__seed_by_block OR __seed_by_block_group) THEN
    RETURN;
  END IF;

  -- The seed counts only what a fork cannot revert, from history up to the
  -- context's irreversible block, inserted before app_register_table and so
  -- without shadow rows: a row created up to it counts as created, and as filled
  -- or cancelled only when it also ended up to it. The rows get distinct
  -- hive_rowid values, which app_register_table would otherwise add as 0 to every
  -- one: HAF reverts an update by hive_rowid, so a fork reverting a later block's
  -- update of one seeded row would rewrite them all. The row id sequence
  -- registration creates is then moved past them.
  -- A detached context's irreversible block is frozen (only attach moves it) and
  -- may be 0, but everything it processed up to HAF's consistent block is
  -- irreversible and it records no change for a fork to revert: the seed counts
  -- all of it, so its reversible part below is empty.
  -- The context's row stays locked to the end of the transaction, so the app
  -- cannot move its current block between the reads of it here and below and the
  -- restore of it, and order_lifecycle holds exactly the blocks up to it. The
  -- app holds it for a whole batch, which can run for minutes in massive sync:
  -- the wait is bounded, and the install fails then rather than hang.
  PERFORM set_config('lock_timeout', '5min', true);
  BEGIN
    PERFORM 1 FROM hafd.contexts WHERE name = __schema_name FOR UPDATE;
  EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'btracker: context % stayed locked by its running application for over % (lock_timeout): cannot seed order_stats_by_block and order_stats_by_block_group',
      __schema_name, current_setting('lock_timeout')
      USING ERRCODE = 'lock_not_available',
            HINT = 'Stop the application, or retry the install once it is between batches.';
  END;
  __irreversible := hive.app_get_irreversible_block(__schema_name);
  IF NOT hive.app_context_is_attached(__schema_name) THEN
    __irreversible := GREATEST(__irreversible, LEAST(
      hive.app_get_current_block_num(__schema_name),
      (SELECT hs.consistent_block FROM hafd.hive_state hs)));
  END IF;

  IF __seed_by_block THEN
    CREATE TABLE order_stats_by_block
    (
      block_created INT NOT NULL,
      created       INT NOT NULL, -- Orders created in the block
      filled        INT NOT NULL, -- Of them, the ones that ended filled
      cancelled     INT NOT NULL, -- Of them, the ones that ended cancelled
      hive_rowid    BIGINT NOT NULL DEFAULT 0,

      CONSTRAINT pk_order_stats_by_block PRIMARY KEY (block_created)
    );
    INSERT INTO order_stats_by_block (hive_rowid, block_created, created, filled, cancelled)
    SELECT row_number() OVER (), block_created, count(*),
           count(*) FILTER (WHERE outcome = 'filled' AND end_block <= __irreversible),
           count(*) FILTER (WHERE outcome = 'cancelled' AND end_block <= __irreversible)
    FROM order_lifecycle
    WHERE block_created <= __irreversible
    GROUP BY block_created;
    PERFORM hive.app_register_table( __schema_name, 'order_stats_by_block', __schema_name );
    PERFORM setval(pg_get_serial_sequence('order_stats_by_block', 'hive_rowid'),
                   (SELECT COALESCE(max(hive_rowid), 0) + 1 FROM order_stats_by_block), false);
  END IF;

  IF __seed_by_block_group THEN
    CREATE TABLE order_stats_by_block_group
    (
      block_group INT NOT NULL, -- block_created / btracker_backend.order_stats_group_blocks()
      created     INT NOT NULL,
      filled      INT NOT NULL,
      cancelled   INT NOT NULL,
      hive_rowid  BIGINT NOT NULL DEFAULT 0,

      CONSTRAINT pk_order_stats_by_block_group PRIMARY KEY (block_group)
    );
    INSERT INTO order_stats_by_block_group (hive_rowid, block_group, created, filled, cancelled)
    SELECT row_number() OVER (), block_created / __group_blocks, count(*),
           count(*) FILTER (WHERE outcome = 'filled' AND end_block <= __irreversible),
           count(*) FILTER (WHERE outcome = 'cancelled' AND end_block <= __irreversible)
    FROM order_lifecycle
    WHERE block_created <= __irreversible
    GROUP BY block_created / __group_blocks;
    PERFORM hive.app_register_table( __schema_name, 'order_stats_by_block_group', __schema_name );
    PERFORM setval(pg_get_serial_sequence('order_stats_by_block_group', 'hive_rowid'),
                   (SELECT COALESCE(max(hive_rowid), 0) + 1 FROM order_stats_by_block_group), false);
  END IF;

  -- The rest, the creations and outcomes of the reversible blocks the context
  -- already processed, is added now that the tables are registered, one block at
  -- a time with the context's current block set to it, as processing that block
  -- would have added it: HAF's triggers record each change at that block, so a
  -- fork back to any block reverts the counts of exactly the blocks it reverts in
  -- order_lifecycle, and the blocks processed again after it add them back.
  -- Inserted at the context's current block in one statement instead, a fork to
  -- a block below it would also revert the counts of the blocks it keeps.
  -- hive.app_set_current_block_num refuses an attached context, hence the
  -- UPDATE; the current block is restored before the transaction ends.
  __current_block := hive.app_get_current_block_num(__schema_name);

  CREATE TEMP TABLE _order_stats_reversible ON COMMIT DROP AS
  SELECT e.event_block, ol.block_created,
         count(*) FILTER (WHERE e.is_create) AS created,
         count(*) FILTER (WHERE NOT e.is_create AND ol.outcome = 'filled') AS filled,
         count(*) FILTER (WHERE NOT e.is_create AND ol.outcome = 'cancelled') AS cancelled
  FROM order_lifecycle ol
  CROSS JOIN LATERAL (VALUES (ol.block_created, TRUE), (ol.end_block, FALSE)) AS e(event_block, is_create)
  WHERE e.event_block > __irreversible
  GROUP BY e.event_block, ol.block_created;

  -- One ordered walk over the rows, moving the current block at each new
  -- event block: linear in the rows, however many blocks they span.
  FOR __reversible IN SELECT * FROM _order_stats_reversible ORDER BY event_block
  LOOP
    IF __reversible.event_block IS DISTINCT FROM __event_block THEN
      __event_block := __reversible.event_block;
      UPDATE hafd.contexts SET current_block_num = __event_block WHERE name = __schema_name;
    END IF;
    IF __seed_by_block THEN
      INSERT INTO order_stats_by_block AS s (block_created, created, filled, cancelled)
      VALUES (__reversible.block_created, __reversible.created, __reversible.filled, __reversible.cancelled)
      ON CONFLICT (block_created) DO UPDATE
        SET created   = s.created + EXCLUDED.created,
            filled    = s.filled + EXCLUDED.filled,
            cancelled = s.cancelled + EXCLUDED.cancelled;
    END IF;
    IF __seed_by_block_group THEN
      INSERT INTO order_stats_by_block_group AS s (block_group, created, filled, cancelled)
      VALUES (__reversible.block_created / __group_blocks, __reversible.created, __reversible.filled, __reversible.cancelled)
      ON CONFLICT (block_group) DO UPDATE
        SET created   = s.created + EXCLUDED.created,
            filled    = s.filled + EXCLUDED.filled,
            cancelled = s.cancelled + EXCLUDED.cancelled;
    END IF;
  END LOOP;
  UPDATE hafd.contexts SET current_block_num = __current_block
  WHERE name = __schema_name AND current_block_num IS DISTINCT FROM __current_block;
  DROP TABLE _order_stats_reversible;
END
$$;

RESET ROLE;
