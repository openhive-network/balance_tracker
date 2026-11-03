/*
 * finalize_security_definer.sql
 * =============================
 * finalize_massive_sync() and finalize_massive_sync_before_forking() run as
 * btracker_owner (SECURITY DEFINER):
 *
 *   - a caller with a schema of its own first in its search_path, holding a
 *     function named isIndexesCreated, does not get that function called;
 *   - PUBLIC holds no EXECUTE on them, and btracker_user, which has USAGE on the
 *     schema, is refused.
 *
 * Expects psql variable `schema` (the application schema) and a synced, finalized
 * schema. Runs in a transaction that is rolled back. Run under
 * `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;

BEGIN;
DO
$$
DECLARE
  __schema hive.context_name := current_schema();
  __function TEXT;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  FOREACH __function IN ARRAY ARRAY['finalize_massive_sync_before_forking', 'finalize_massive_sync'] LOOP
    IF EXISTS (
      SELECT 1
      FROM pg_proc p, aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a
      WHERE p.oid = format('%I.%I(hive.context_name)', __schema, __function)::regprocedure
        AND a.grantee = 0 AND a.privilege_type = 'EXECUTE'
    ) THEN
      RAISE EXCEPTION '%: PUBLIC may execute it', __function;
    END IF;
  END LOOP;

  SET LOCAL ROLE btracker_owner;
  CREATE SCHEMA btracker_test_shadow;
  CREATE FUNCTION btracker_test_shadow.isIndexesCreated() RETURNS BOOLEAN LANGUAGE plpgsql AS
    $f$ BEGIN RAISE EXCEPTION 'shadowing isIndexesCreated() was called'; END $f$;
  PERFORM set_config('search_path', format('btracker_test_shadow, %I, public', __schema), true);

  BEGIN
    PERFORM isIndexesCreated();
    RAISE EXCEPTION 'the shadowing function is not first in the search_path';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'shadowing isIndexesCreated() was called' THEN
      RAISE;
    END IF;
  END;

  PERFORM finalize_massive_sync_before_forking(__schema);
  PERFORM finalize_massive_sync(__schema);
  RESET ROLE;
  PERFORM set_config('search_path', format('%I, public', __schema), true);

  SET LOCAL ROLE btracker_user;
  FOREACH __function IN ARRAY ARRAY['finalize_massive_sync_before_forking', 'finalize_massive_sync'] LOOP
    BEGIN
      EXECUTE format('SELECT %I.%I($1)', __schema, __function) USING __schema;
      RAISE EXCEPTION '%: btracker_user is not refused', __function;
    EXCEPTION WHEN insufficient_privilege THEN
      IF SQLERRM NOT LIKE 'permission denied for function%' THEN
        RAISE;
      END IF;
    END;
  END LOOP;
  RESET ROLE;

  RAISE NOTICE 'PASS: finalize functions pin their search_path and refuse ungranted roles';
END
$$;
ROLLBACK;
