/*
 * endpoint_volatility_check.sql
 * =============================
 * No function in btracker_endpoints, the schema PostgREST exposes, is VOLATILE.
 * The postgrest-rewriter forwards every GET to PostgREST as a POST, and PostgREST
 * runs a POST to a VOLATILE function in a READ WRITE transaction; a STABLE or
 * IMMUTABLE one runs read-only.
 *
 * Fails naming each VOLATILE function. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on

DO
$$
DECLARE
  __volatile TEXT;
BEGIN
  SELECT string_agg(p.oid::regprocedure::TEXT, ', ' ORDER BY p.oid::regprocedure::TEXT)
  INTO __volatile
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'btracker_endpoints'
    AND p.provolatile = 'v';

  IF __volatile IS NOT NULL THEN
    RAISE EXCEPTION 'VOLATILE functions in btracker_endpoints (declare them STABLE): %', __volatile;
  END IF;
END
$$;
