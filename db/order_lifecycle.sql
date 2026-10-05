SET ROLE btracker_owner;

/*
 * Install outside btracker_app.sql's initial-context DO block: that block returns
 * early on upgrades. Both tables belong to the existing application context.
 * The status row is a baseline, seeded before registration so a first detach
 * cannot undo it. Its single initial hive_rowid=0 is unambiguous for HAF rewind.
 */
CREATE OR REPLACE FUNCTION install_order_lifecycle(_context_name hive.context_name)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
AS
$$
DECLARE
  __schema TEXT;
  __context_id INT;
  __current_block INT;
  __registered_context INT;
BEGIN
  SELECT schema, id, current_block_num
  INTO __schema, __context_id, __current_block
  FROM hafd.contexts
  WHERE name = _context_name;

  IF NOT FOUND OR __schema IS DISTINCT FROM current_schema() THEN
    RAISE EXCEPTION 'Order lifecycle installation requires the owning context schema: %', _context_name;
  END IF;

  CREATE TABLE IF NOT EXISTS order_lifecycle
  (
    create_op_id   BIGINT   PRIMARY KEY,
    owner_id       INT      NOT NULL,
    order_id       BIGINT   NOT NULL,
    nai            SMALLINT NOT NULL,
    initial_amount BIGINT   NOT NULL,
    -- Unfilled amount; a canceled row retains the amount refunded, not a lock.
    remaining      BIGINT   NOT NULL,
    block_created  INT      NOT NULL,
    created_at     TIMESTAMP NOT NULL,
    outcome        TEXT,
    terminal_op_id BIGINT,
    terminal_block INT,

    CHECK (initial_amount > 0 AND remaining >= 0 AND remaining <= initial_amount),
    CHECK (
      (outcome IS NULL AND terminal_op_id IS NULL AND terminal_block IS NULL)
      OR
      (outcome IS NOT NULL AND outcome IN ('filled', 'canceled')
        AND terminal_op_id IS NOT NULL AND terminal_block IS NOT NULL)
    ),
    CHECK (outcome IS DISTINCT FROM 'filled' OR remaining = 0),
    CHECK (terminal_op_id > create_op_id),
    CHECK (terminal_block >= block_created)
  );

  -- A correctness index, needed during massive processing as well as LIVE.
  -- Do not add it to HAF's droppable read-index dependencies.
  CREATE UNIQUE INDEX IF NOT EXISTS order_lifecycle_active_key_idx
    ON order_lifecycle(owner_id, order_id) WHERE outcome IS NULL;
  CREATE INDEX IF NOT EXISTS order_lifecycle_owner_created_idx
    ON order_lifecycle(owner_id, created_at, block_created);
  CREATE INDEX IF NOT EXISTS order_lifecycle_created_idx
    ON order_lifecycle(created_at, block_created);

  SELECT context_id INTO __registered_context
  FROM hafd.registered_tables
  WHERE origin_table_schema = __schema AND origin_table_name = 'order_lifecycle';
  IF FOUND THEN
    IF __registered_context <> __context_id THEN
      RAISE EXCEPTION 'Order lifecycle table belongs to a different HAF context';
    END IF;
  ELSE
    -- app_register_table initializes existing rows with the same hive_rowid=0.
    -- Register the history empty so every incarnation receives its own row ID.
    IF EXISTS (SELECT 1 FROM order_lifecycle) THEN
      RAISE EXCEPTION 'Cannot register a populated, unregistered order lifecycle table';
    END IF;
    PERFORM hive.app_register_table(__schema, 'order_lifecycle', _context_name);
  END IF;

  CREATE TABLE IF NOT EXISTS order_lifecycle_status
  (
    singleton         BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    processed_through INT     NOT NULL DEFAULT 0 CHECK (processed_through >= 0),
    indexed_at        TIMESTAMP,
    backfill_required BOOLEAN NOT NULL,
    backfill_target   INT CHECK (backfill_target >= 0),
    schema_version    INT     NOT NULL DEFAULT 1,
    CHECK ((processed_through = 0 AND indexed_at IS NULL)
      OR (processed_through > 0 AND indexed_at IS NOT NULL))
  );
  -- Even an INSERT with zero rows fires HAF's statement trigger, whose block
  -- guard rejects an attached context at block zero. Skip the statement itself.
  IF NOT EXISTS (SELECT 1 FROM order_lifecycle_status WHERE singleton) THEN
    INSERT INTO order_lifecycle_status(singleton, processed_through, backfill_required)
    VALUES (TRUE, 0, __current_block > 0);
  END IF;

  IF EXISTS (SELECT 1 FROM order_lifecycle_status WHERE schema_version <> 1) THEN
    RAISE EXCEPTION 'Unsupported order lifecycle schema version';
  END IF;

  SELECT context_id INTO __registered_context
  FROM hafd.registered_tables
  WHERE origin_table_schema = __schema AND origin_table_name = 'order_lifecycle_status';
  IF FOUND THEN
    IF __registered_context <> __context_id THEN
      RAISE EXCEPTION 'Order lifecycle status belongs to a different HAF context';
    END IF;
  ELSE
    PERFORM hive.app_register_table(__schema, 'order_lifecycle_status', _context_name);
  END IF;
END
$$;

/*
 * One row per creation, including immediately filled orders and reused IDs.
 * HAF serializes pre-apply notifications, so the create ID precedes nested
 * fill_order/limit_order_cancelled IDs. Never squash this stream to latest create.
 * Both the data and coverage watermark are registered and change atomically.
 *
 * Modern hived replay emits virtual cancellation even for historical expiry and
 * dust. It is authoritative; real cancel is validated and must receive its vop.
 * HF23 account clearing suppresses that vop, so hardfork_hive is handled too.
 * Expiry, dust, explicit cancellation and HF23 removal share the Canceled bucket.
 *
 * The ordered loop favors a correct, reviewable baseline. A later bulk reducer
 * must preserve every incarnation boundary and prove parity with this behavior.
 */
CREATE OR REPLACE FUNCTION process_order_lifecycle(_from INT, _to INT)
RETURNS VOID
LANGUAGE plpgsql VOLATILE
SET jit = OFF
AS
$$
DECLARE
  __status RECORD;
  __op RECORD;
  __event RECORD;
  __active RECORD;
  __owner_id INT;
  __remaining BIGINT;
  __created_at TIMESTAMP;
  __indexed_at TIMESTAMP;
  __last_op BIGINT;
  __pending_cancels BIGINT[] := ARRAY[]::BIGINT[];
  __backfill BOOLEAN := COALESCE(current_setting('btracker.order_lifecycle_backfill', TRUE), '') = 'on';
  __create1 INT := btracker_backend.op_limit_order_create();
  __create2 INT := btracker_backend.op_limit_order_create2();
  __fill INT := btracker_backend.op_fill_order();
  __cancel INT := btracker_backend.op_limit_order_cancel();
  __cancelled INT := btracker_backend.op_limit_order_cancelled();
  __hardfork_hive INT := btracker_backend.op_hardfork_hive();
BEGIN
  IF _from IS NULL OR _to IS NULL OR _from < 1 OR _to < _from THEN
    RAISE EXCEPTION 'Invalid order lifecycle block range: <%, %>', _from, _to;
  END IF;

  SELECT * INTO __status FROM order_lifecycle_status WHERE singleton FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Order lifecycle status is missing; run installation first';
  END IF;
  IF __status.schema_version <> 1 THEN
    RAISE EXCEPTION 'Unsupported order lifecycle schema version';
  END IF;
  IF __status.processed_through <> _from - 1 THEN
    RAISE EXCEPTION 'Order lifecycle range must start at %, received %', __status.processed_through + 1, _from;
  END IF;

  IF __backfill THEN
    IF NOT __status.backfill_required OR __status.backfill_target IS NULL
      OR _to > __status.backfill_target THEN
      RAISE EXCEPTION 'Order lifecycle backfill requires a pending, fixed target covering block %', _to;
    END IF;
    IF hive.app_context_is_attached(current_schema()) THEN
      RAISE EXCEPTION 'Detach the owning HAF context group before order lifecycle backfill';
    END IF;
  ELSIF __status.backfill_required THEN
    RAISE EXCEPTION 'Order lifecycle history requires backfill before normal block processing';
  END IF;

  -- Persist timestamps while the source is available: later HAF pruning must
  -- not remove creation cohorts or the index watermark's freshness metadata.
  SELECT created_at INTO __indexed_at FROM blocks_view WHERE num = _to;
  IF NOT FOUND OR __indexed_at IS NULL THEN
    RAISE EXCEPTION 'Missing source timestamp for order lifecycle block %', _to;
  END IF;

  FOR __op IN
    SELECT id, block_num, op_type_id, body_value
    FROM _btracker_ops_batch
    WHERE block_num BETWEEN _from AND _to
      AND op_type_id IN (__create1, __create2, __fill, __cancel, __cancelled, __hardfork_hive)
    ORDER BY id
  LOOP
    IF __last_op IS NOT NULL AND __op.id <= __last_op THEN
      RAISE EXCEPTION 'Duplicate or unordered order lifecycle operation: %', __op.id;
    END IF;
    __last_op := __op.id;

    IF __op.op_type_id = __hardfork_hive THEN
      SELECT id INTO __owner_id FROM accounts_view WHERE name = __op.body_value ->> 'account';
      IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown cleared order owner at operation %', __op.id;
      END IF;
      -- other_affected_accounts contains delegatees; their orders are not cleared.
      UPDATE order_lifecycle SET outcome = 'canceled', terminal_op_id = __op.id,
        terminal_block = __op.block_num
      WHERE owner_id = __owner_id AND outcome IS NULL;
      CONTINUE;
    END IF;

    IF __op.op_type_id IN (__create1, __create2) THEN
      SELECT * INTO __event FROM btracker_backend.get_limit_order_create_event(__op.body_value);
      SELECT id INTO __owner_id FROM accounts_view WHERE name = __event.owner;
      IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown order creator % at operation %', __event.owner, __op.id;
      END IF;
      IF __event.order_id IS NULL OR __event.nai IS NULL OR __event.amount IS NULL OR __event.amount <= 0 THEN
        RAISE EXCEPTION 'Malformed order creation at operation %', __op.id;
      END IF;
      SELECT created_at INTO __created_at FROM blocks_view WHERE num = __op.block_num;
      IF NOT FOUND OR __created_at IS NULL THEN
        RAISE EXCEPTION 'Missing creation timestamp for order lifecycle operation %', __op.id;
      END IF;
      INSERT INTO order_lifecycle(create_op_id, owner_id, order_id, nai, initial_amount, remaining, block_created, created_at)
      VALUES (__op.id, __owner_id, __event.order_id, __event.nai, __event.amount, __event.amount, __op.block_num, __created_at);
      CONTINUE;
    END IF;

    IF __op.op_type_id = __fill THEN
      IF __op.body_value ->> 'open_owner' IS NULL OR __op.body_value ->> 'open_orderid' IS NULL
        OR __op.body_value -> 'open_pays' IS NULL OR __op.body_value ->> 'current_owner' IS NULL
        OR __op.body_value ->> 'current_orderid' IS NULL OR __op.body_value -> 'current_pays' IS NULL THEN
        RAISE EXCEPTION 'Malformed order fill at operation %', __op.id;
      END IF;
      FOR __event IN SELECT * FROM btracker_backend.get_limit_order_fill_events(__op.body_value)
      LOOP
        SELECT id INTO __owner_id FROM accounts_view WHERE name = __event.owner;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'Unknown filled order owner % at operation %', __event.owner, __op.id;
        END IF;
        SELECT * INTO __active FROM order_lifecycle
        WHERE owner_id = __owner_id AND order_id = __event.order_id AND outcome IS NULL FOR UPDATE;
        IF NOT FOUND THEN
          RAISE EXCEPTION 'No active order %/% for fill operation %', __event.owner, __event.order_id, __op.id;
        END IF;
        -- Integer price rounding can produce a zero-paying side; hived's market
        -- matcher explicitly allows it. It consumes no amount on this side.
        IF __event.nai IS DISTINCT FROM __active.nai OR __event.amount IS NULL OR __event.amount < 0
          OR __event.amount > __active.remaining THEN
          RAISE EXCEPTION 'Invalid fill asset or amount for order %/% at operation %', __event.owner, __event.order_id, __op.id;
        END IF;
        __remaining := __active.remaining - __event.amount;
        UPDATE order_lifecycle SET remaining = __remaining,
          outcome = CASE WHEN __remaining = 0 THEN 'filled' END,
          terminal_op_id = CASE WHEN __remaining = 0 THEN __op.id END,
          terminal_block = CASE WHEN __remaining = 0 THEN __op.block_num END
        WHERE create_op_id = __active.create_op_id;
      END LOOP;
      CONTINUE;
    END IF;

    SELECT * INTO __event FROM btracker_backend.get_limit_order_cancel_event(__op.body_value);
    SELECT id INTO __owner_id FROM accounts_view WHERE name = __event.owner;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'Unknown canceled order owner % at operation %', __event.owner, __op.id;
    END IF;
    SELECT * INTO __active FROM order_lifecycle
    WHERE owner_id = __owner_id AND order_id = __event.order_id AND outcome IS NULL FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'No active order %/% for cancel operation %', __event.owner, __event.order_id, __op.id;
    END IF;

    IF __op.op_type_id = __cancel THEN
      __pending_cancels := array_append(__pending_cancels, __active.create_op_id);
    ELSE
      -- The refund verifies our remaining amount without inferring dust/expiry.
      IF (__op.body_value -> 'amount_back') IS NULL THEN
        RAISE EXCEPTION 'Missing order cancellation refund at operation %', __op.id;
      END IF;
      SELECT * INTO __event FROM btracker_backend.parse_amount_object(__op.body_value -> 'amount_back');
      IF __event.asset_symbol_nai IS DISTINCT FROM __active.nai
        OR __event.amount IS DISTINCT FROM __active.remaining THEN
        RAISE EXCEPTION 'Cancellation refund does not match order state at operation %', __op.id;
      END IF;
      UPDATE order_lifecycle SET outcome = 'canceled', terminal_op_id = __op.id,
        terminal_block = __op.block_num
      WHERE create_op_id = __active.create_op_id;
      __pending_cancels := array_remove(__pending_cancels, __active.create_op_id);
    END IF;
  END LOOP;

  IF cardinality(__pending_cancels) > 0 THEN
    RAISE EXCEPTION 'Missing virtual cancellation for order lifecycle creations: %', __pending_cancels;
  END IF;

  UPDATE order_lifecycle_status SET processed_through = _to, indexed_at = __indexed_at WHERE singleton;
END
$$;

RESET ROLE;
