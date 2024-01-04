-------- hive_operations_is_saved_into_hbd_balance
DO $$
  BEGIN
    IF EXISTS(SELECT 1 FROM pg_index WHERE NOT indisvalid AND indexrelid = (SELECT oid FROM pg_class WHERE relname = 'hive_operations_is_saved_into_hbd_balance')) THEN
      RAISE NOTICE 'Dropping invalid index hive_operations_is_saved_into_hbd_balance, it will be recreated';
      DROP INDEX hive.hive_operations_is_saved_into_hbd_balance;
    END IF;
  END
$$;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hive_operations_is_saved_into_hbd_balance ON hive.operations USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'is_saved_into_hbd_balance')::boolean)
)
WHERE op_type_id = 55;

-------- hive_operations_reversible_is_saved_into_hbd_balance
DO $$
  BEGIN
    IF EXISTS(SELECT 1 FROM pg_index WHERE NOT indisvalid AND indexrelid = (SELECT oid FROM pg_class WHERE relname = 'hive_operations_reversible_is_saved_into_hbd_balance')) THEN
      RAISE NOTICE 'Dropping invalid index hive_operations_reversible_is_saved_into_hbd_balance, it will be recreated';
      DROP INDEX hive.hive_operations_reversible_is_saved_into_hbd_balance;
    END IF;
  END
$$;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hive_operations_reversible_is_saved_into_hbd_balance
ON hive.operations_reversible USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'is_saved_into_hbd_balance')::boolean)
)
WHERE op_type_id = 55;


-------- Indexes for reward_operations:
-------- hive_operations_payout_must_be_claimed
DO $$
  BEGIN
    IF EXISTS(SELECT 1 FROM pg_index WHERE NOT indisvalid AND indexrelid = (SELECT oid FROM pg_class WHERE relname = 'hive_operations_payout_must_be_claimed')) THEN
      RAISE NOTICE 'Dropping invalid index hive_operations_payout_must_be_claimed, it will be recreated';
      DROP INDEX hive.hive_operations_payout_must_be_claimed;
    END IF;
  END
$$;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hive_operations_payout_must_be_claimed ON hive.operations USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'payout_must_be_claimed')::boolean)
)
WHERE op_type_id IN (51, 63);

-------- hive_operations_reversible_payout_must_be_claimed
DO $$
  BEGIN
    IF EXISTS(SELECT 1 FROM pg_index WHERE NOT indisvalid AND indexrelid = (SELECT oid FROM pg_class WHERE relname = 'hive_operations_reversible_payout_must_be_claimed')) THEN
      RAISE NOTICE 'Dropping invalid index hive_operations_reversible_payout_must_be_claimed, it will be recreated';
      DROP INDEX hive.hive_operations_reversible_payout_must_be_claimed;
    END IF;
  END
$$;
CREATE INDEX CONCURRENTLY IF NOT EXISTS hive_operations_reversible_payout_must_be_claimed ON hive.operations_reversible
USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'payout_must_be_claimed')::boolean)
)
WHERE op_type_id IN (51, 63);
