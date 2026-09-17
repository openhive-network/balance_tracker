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
 * Same schema-injection ceremony as last_synced_block() above.
 *
 * The block's timestamp comes from HAF's public hive.get_app_current_block_age()
 * rather than from hafd.blocks (which only holds irreversible blocks and so
 * returned a null time for a forking context's freshly processed head block)
 * or from the context's blocks_view (which HAF recreates under an ACCESS
 * EXCLUSIVE lock on every context attach/detach, stalling the endpoint behind
 * the app's iteration transaction whenever the app catches up). See the
 * comment in the function body.
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
    DECLARE
      __block_num INT := (SELECT current_block_num FROM hafd.contexts WHERE name = %1$L);
    BEGIN
      -- Fail fast during HAF massive sync: hafd.blocks' PK is dropped for the
      -- duration (hive.disable_indexes_of_irreversible), so the lookup below
      -- would seq-scan the largest table in the database. Health-check agents
      -- gate on is_instance_ready() before calling APIs; this guard protects
      -- any caller that does not (e.g. a raw haproxy httpchk) by erroring in
      -- milliseconds instead of stalling.
      IF NOT hive.is_instance_ready() THEN
        RAISE EXCEPTION 'HAF instance is not ready (massive sync in progress)'
          USING ERRCODE = '55000';
      END IF;

      -- Block timestamp via HAF's public API. hive.get_app_current_block_age()
      -- reads hafd.contexts + hafd.blocks + hafd.blocks_reversible, so a freshly
      -- processed, still-reversible head block resolves, and it touches no
      -- context view: HAF recreates <ctx>.blocks_view under an ACCESS EXCLUSIVE
      -- lock on every context attach/detach (which the HAF app loop does when
      -- switching stages to catch up), so a lookup through the view queues
      -- behind the app's whole iteration transaction (seen: 17 s -> statement
      -- timeouts and haproxy check failures). now() is the transaction
      -- timestamp on both sides of the subtraction, so now() - age is the
      -- block's created_at exactly (HAF runs with a UTC session time zone).
      -- Block 0 (pre-sync) has no row and HAF reports its age from the epoch,
      -- hence the explicit null.
      RETURN json_build_object(
        'last_block_num', __block_num,
        'last_block_time', CASE WHEN __block_num > 0 THEN
          to_char(now() - hive.get_app_current_block_age(ARRAY[%1$L]::hive.contexts_group),
                  'YYYY-MM-DD"T"HH24:MI:SS')
        END
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
