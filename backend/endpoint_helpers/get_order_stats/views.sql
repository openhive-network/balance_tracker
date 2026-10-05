SET ROLE btracker_owner;

-- Timestamps are persisted by the reducer, so retained lifecycle history stays
-- queryable even when HAF later prunes its raw creation blocks.
CREATE OR REPLACE VIEW btracker_backend.order_lifecycle_view AS
SELECT
    t.create_op_id,
    t.owner_id,
    t.order_id,
    t.nai,
    t.initial_amount,
    t.remaining,
    t.block_created,
    t.created_at,
    t.outcome,
    t.terminal_op_id,
    t.terminal_block
FROM order_lifecycle AS t;

DO $$
DECLARE
  __schema TEXT := current_schema();
BEGIN
  EXECUTE format($body$
    CREATE OR REPLACE VIEW btracker_backend.order_lifecycle_progress_view AS
      SELECT t.*, %1$L::TEXT AS context_name,
        btracker_backend.last_synced_block() AS context_head
      FROM %1$I.order_lifecycle_status t
  $body$, __schema);
END
$$;

RESET ROLE;
