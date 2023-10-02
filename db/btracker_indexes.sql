CREATE INDEX IF NOT EXISTS hive_operations_is_saved_into_hbd_balance ON hive.operations USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'is_saved_into_hbd_balance')::boolean)
)
WHERE op_type_id = 55;

CREATE INDEX IF NOT EXISTS hive_operations_reversible_is_saved_into_hbd_balance
ON hive.operations_reversible USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'is_saved_into_hbd_balance')::boolean)
)
WHERE op_type_id = 55;

--Indexes for reward_operations
CREATE INDEX IF NOT EXISTS hive_operations_payout_must_be_claimed ON hive.operations USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'payout_must_be_claimed')::boolean)
)
WHERE op_type_id IN (51, 63);

CREATE INDEX IF NOT EXISTS hive_operations_reversible_payout_must_be_claimed ON hive.operations_reversible USING btree
(
    ((body_binary::jsonb -> 'value' ->> 'payout_must_be_claimed')::boolean)
)
WHERE op_type_id IN (51, 63);
