SET ROLE btracker_owner;

/*
 * Cache-Control header for history/stats endpoints.
 * --------------------------------------------------------------------------
 * Long cache (1 year) once the queried upper bound is fully irreversible, short
 * cache (2s) while the range can still change. Replaces the identical IF block
 * repeated verbatim across every read endpoint (vesting/balance/transfer history
 * and stats). PostgREST reads `response.headers` from the GUC set here.
 */
CREATE OR REPLACE FUNCTION btracker_backend.set_history_cache_headers(_last_block INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  IF _last_block <= hive.app_get_irreversible_block() AND _last_block IS NOT NULL THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;
END
$$;

RESET ROLE;
