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

/*
 * sync_status() — the last processed block as {last_block_num, last_block_time},
 * for the /sync-status endpoint (the HAF-wide uniform health/freshness API that
 * supersedes the bare-integer /last-synced-block). The timestamp lets consumers
 * compute staleness with a single call (age = now() - last_block_time) instead
 * of needing a second head-block reference.
 * Same schema-injection ceremony as last_synced_block() above; LEFT JOIN so the
 * pre-sync case (no processed block yet) still yields an object with null time.
 */
DO $$
DECLARE
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  EXECUTE format(
  $BODY$
    CREATE OR REPLACE FUNCTION btracker_backend.sync_status()
    RETURNS JSON
    LANGUAGE 'plpgsql' STABLE
    AS
    $pb$
    BEGIN
      -- Fail fast during HAF massive sync: hafd.blocks' PK is dropped for the
      -- duration (hive.disable_indexes_of_irreversible), so the join below
      -- would seq-scan the largest table in the database. Health-check agents
      -- gate on is_instance_ready() before calling APIs; this guard protects
      -- any caller that does not (e.g. a raw haproxy httpchk) by erroring in
      -- milliseconds instead of stalling.
      IF NOT hive.is_instance_ready() THEN
        RAISE EXCEPTION 'HAF instance is not ready (massive sync in progress)'
          USING ERRCODE = '55000';
      END IF;

      RETURN (
        SELECT json_build_object(
          'last_block_num', c.current_block_num,
          'last_block_time', to_char(b.created_at, 'YYYY-MM-DD"T"HH24:MI:SS')
        )
        FROM hafd.contexts c
        LEFT JOIN hafd.blocks b ON b.num = c.current_block_num
        WHERE c.name = '%s'
      );
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
