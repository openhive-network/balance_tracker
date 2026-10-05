SET ROLE btracker_owner;

-- Run on a dedicated connection (scripts/backfill_order_lifecycle.sh). The
-- session install lock survives each batch COMMIT and ends with that connection.
CREATE OR REPLACE PROCEDURE backfill_order_lifecycle(
    _context_name hive.context_name,
    _batch_size INT = 10000,
    _app_lock_name TEXT = NULL
)
LANGUAGE plpgsql
AS
$$
DECLARE
  __contexts hive.contexts_group := ARRAY[_context_name]::hive.contexts_group;
  __group_count INT;
  __attached_count INT;
  __head INT;
  __status RECORD;
  __block_count BIGINT;
  __first INT;
  __last INT;
  __batch_end INT;
  __lock_name TEXT := COALESCE(_app_lock_name,
    CASE WHEN _context_name = 'hafbe_bal' THEN 'haf_block_explorer' ELSE 'balance_tracker' END);
BEGIN
  IF _batch_size IS NULL OR _batch_size < 1 OR _batch_size > 100000 THEN
    RAISE EXCEPTION 'batch-size must be between 1 and 100000';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM hafd.contexts WHERE name = _context_name AND schema = current_schema()) THEN
    RAISE EXCEPTION 'Backfill must run in the owning context schema: %', _context_name;
  END IF;
  IF hive.is_pruning_enabled() THEN
    RAISE EXCEPTION 'Disable HAF pruning before lifecycle backfill';
  END IF;
  -- Each transaction after the first COMMIT reads a consistent source snapshot,
  -- including its block-coverage check and operation rows. This is a dedicated
  -- maintenance connection, so the session default ends with the command.
  PERFORM set_config('default_transaction_isolation', 'repeatable read', FALSE);
  IF NOT hive.try_acquire_app_install_lock(__lock_name) THEN
    RAISE EXCEPTION 'Application processor is active; stop it before lifecycle backfill'
      USING ERRCODE = '55P03';
  END IF;

  SELECT COUNT(*) INTO __group_count
  FROM hafd.applications a WHERE _context_name::TEXT = ANY(a.contexts);
  IF __group_count > 1 THEN
    RAISE EXCEPTION 'Context belongs to multiple application groups';
  ELSIF __group_count = 1 THEN
    SELECT a.contexts INTO __contexts
    FROM hafd.applications a WHERE _context_name::TEXT = ANY(a.contexts);
  ELSE
    IF _context_name = 'hafbe_bal' THEN
      RAISE EXCEPTION 'Embedded tracker requires its registered parent context group';
    END IF;
    __contexts := ARRAY[_context_name]::hive.contexts_group;
  END IF;

  SELECT * INTO STRICT __status FROM order_lifecycle_status WHERE singleton FOR UPDATE;
  __head := hive.app_get_current_block_num(__contexts);
  IF NOT __status.backfill_required THEN
    IF __status.processed_through <> __head THEN
      RAISE EXCEPTION 'Ready lifecycle index does not match the context cursor';
    END IF;
    RAISE NOTICE 'Order lifecycle history already covers block %', __head;
    RETURN;
  END IF;

  SELECT COUNT(*) INTO __block_count FROM blocks_view WHERE num BETWEEN 1 AND __head;
  IF __block_count <> __head THEN
    RAISE EXCEPTION 'Complete retained block history is required through block %', __head;
  END IF;

  SELECT COUNT(*) FILTER (WHERE hive.app_context_is_attached(c))
    INTO __attached_count FROM unnest(__contexts) c;
  IF __attached_count <> 0 AND __attached_count <> cardinality(__contexts) THEN
    RAISE EXCEPTION 'Application context group is only partially attached';
  END IF;
  IF __status.backfill_target IS NULL THEN
    IF __attached_count > 0 THEN
      -- HAF rewinds the complete group to its irreversible baseline; all other
      -- projections retain that baseline and replay the reversible tail later.
      PERFORM hive.app_context_detach(__contexts);
    END IF;
    __head := hive.app_get_current_block_num(__contexts);
    SELECT * INTO STRICT __status FROM order_lifecycle_status WHERE singleton FOR UPDATE;
    IF __status.processed_through > __head THEN
      RAISE EXCEPTION 'Lifecycle checkpoint exceeds the detached context cursor';
    END IF;
    UPDATE order_lifecycle_status SET backfill_target = __head WHERE singleton;
    COMMIT;
  ELSE
    IF __attached_count > 0 OR __head <> __status.backfill_target THEN
      RAISE EXCEPTION 'Resume requires the detached group at the recorded backfill target';
    END IF;
    COMMIT;
  END IF;

  SELECT * INTO STRICT __status FROM order_lifecycle_status WHERE singleton;
  -- Verify retained blocks cover genesis through the fixed target, including
  -- empty blocks. Missing history must never be reported as zero creations.
  SELECT COUNT(*), MIN(num), MAX(num)
    INTO __block_count, __first, __last
  FROM blocks_view WHERE num BETWEEN 1 AND __status.backfill_target;
  IF __block_count <> __status.backfill_target
    OR (__status.backfill_target > 0 AND (__first <> 1 OR __last <> __status.backfill_target))
  THEN
    RAISE EXCEPTION 'Complete retained block history is required through block %', __status.backfill_target;
  END IF;
  COMMIT;

  WHILE __status.processed_through < __status.backfill_target LOOP
    __batch_end := LEAST(__status.processed_through::BIGINT + _batch_size,
                        __status.backfill_target)::INT;
    IF hive.app_get_current_block_num(__contexts) <> __status.backfill_target THEN
      RAISE EXCEPTION 'Application context cursor changed during lifecycle backfill';
    END IF;
    IF EXISTS (SELECT 1 FROM unnest(__contexts) c WHERE hive.app_context_is_attached(c)) THEN
      RAISE EXCEPTION 'Lifecycle backfill requires detached application contexts';
    END IF;
    IF hive.is_pruning_enabled() THEN
      RAISE EXCEPTION 'HAF pruning was enabled during lifecycle backfill';
    END IF;
    IF current_setting('transaction_isolation') <> 'repeatable read' THEN
      RAISE EXCEPTION 'Lifecycle backfill batch requires a repeatable-read source snapshot';
    END IF;
    SELECT COUNT(*) INTO __block_count FROM blocks_view
      WHERE num BETWEEN __status.processed_through + 1 AND __batch_end;
    IF __block_count <> __batch_end - __status.processed_through THEN
      RAISE EXCEPTION 'Retained history is incomplete for the next lifecycle batch';
    END IF;

    DROP TABLE IF EXISTS pg_temp._btracker_ops_batch;
    CREATE TEMP TABLE _btracker_ops_batch ON COMMIT DROP AS
      SELECT id, block_num, op_type_id, body_value
      FROM operations_view
      WHERE block_num BETWEEN __status.processed_through + 1 AND __batch_end
        AND op_type_id IN (
          btracker_backend.op_limit_order_create(), btracker_backend.op_limit_order_create2(),
          btracker_backend.op_limit_order_cancel(), btracker_backend.op_limit_order_cancelled(),
          btracker_backend.op_fill_order(), btracker_backend.op_hardfork_hive()
        );
    PERFORM set_config('btracker.order_lifecycle_backfill', 'on', TRUE);
    PERFORM process_order_lifecycle(__status.processed_through + 1, __batch_end);
    COMMIT;
    SELECT * INTO STRICT __status FROM order_lifecycle_status WHERE singleton;
    RAISE NOTICE 'Order lifecycle checkpoint: % / %', __status.processed_through, __status.backfill_target;
  END LOOP;

  IF hive.app_get_current_block_num(__contexts) <> __status.backfill_target THEN
    RAISE EXCEPTION 'Application cursor changed before lifecycle completion';
  END IF;
  -- Reattach and mark ready atomically. Older HAF versions that move the cursor
  -- on attach fail this assertion rather than silently skipping the missing tail.
  PERFORM hive.app_context_attach(__contexts);
  IF hive.app_get_current_block_num(__contexts) <> __status.backfill_target THEN
    RAISE EXCEPTION 'HAF attach changed the context cursor; a cursor-preserving HAF version is required';
  END IF;
  UPDATE order_lifecycle_status SET backfill_required = FALSE, backfill_target = NULL WHERE singleton;
  COMMIT;
  RAISE NOTICE 'Lifecycle backfill complete; restart the owning application processor';
END
$$;

RESET ROLE;
