SET ROLE btracker_owner;

/*
 * last_synced_block() — the block number Balance Tracker has processed
 * (its HAF context's current_block_num).
 * --------------------------------------------------------------------------
 * The HAF context name equals the install schema, so the name is baked in ONCE here
 * via the schema-injection ceremony (DO + EXECUTE format). Every aggregation function
 * and the /last-synced-block endpoint call this helper instead of repeating that whole
 * `DO $$ ... EXECUTE format($BODY$ ... WHERE name = '%s' ... $BODY$, __schema_name)`
 * wrapper just to read one value — which lets those callers be plain CREATE FUNCTIONs.
 */
DO $$
DECLARE
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(
  $BODY$
    CREATE OR REPLACE FUNCTION btracker_backend.last_synced_block()
    RETURNS INT
    LANGUAGE 'plpgsql' STABLE
    AS
    $pb$
    BEGIN
      RETURN current_block_num FROM hafd.contexts WHERE name = '%s';
    END
    $pb$;
  $BODY$, __schema_name);
END
$$;

-- Highest block whose timestamp is at or before _ts. Used by the gap-fill aggregations to
-- attribute a block number to empty time buckets, replacing the repeated LATERAL lookup.
CREATE OR REPLACE FUNCTION btracker_backend.block_at_or_before(_ts TIMESTAMP)
RETURNS INT
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    SELECT b.num
    FROM hive.blocks_view b
    WHERE b.created_at <= _ts
    ORDER BY b.created_at DESC
    LIMIT 1
  );
END
$$;

RESET ROLE;
