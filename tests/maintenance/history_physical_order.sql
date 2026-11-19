/*
 * history_physical_order.sql
 * ==========================
 * After a massive sync and finalize_massive_sync(), account_balance_history and
 * account_savings_history are stored in (account, nai, balance_seq_no) order,
 * the order balance-history pages read them in:
 *
 *   - no row lies on an earlier heap page than the row before it in key order
 *     (so the key-order rewrite was not skipped); an empty
 *     account_savings_history (a synced range before savings existed) is
 *     skipped;
 *   - once HAF has carried out finalize's 'freeze' request for a table, at least
 *     95% of its pages are all-visible (skipped while the request is pending or
 *     on a HAF without request kinds).
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema with no block processed and committed after
 * finalize: rows of a later block are appended at the end of the table. Changes
 * no state. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

DO
$$
DECLARE
  __table TEXT;
  __rows BIGINT;
  __backward BIGINT;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  FOREACH __table IN ARRAY ARRAY['account_balance_history', 'account_savings_history'] LOOP
    EXECUTE format($q$
      SELECT count(*), count(*) FILTER (WHERE page < prev_page)
      FROM (
        SELECT (ctid::TEXT::POINT)[0] AS page,
               lag((ctid::TEXT::POINT)[0]) OVER (ORDER BY account, nai, balance_seq_no) AS prev_page
        FROM %I
      ) s
    $q$, __table) INTO __rows, __backward;

    IF __rows = 0 AND __table = 'account_savings_history' THEN
      RAISE NOTICE 'SKIP: % physical order: the synced range has no savings history', __table;
      CONTINUE;
    END IF;
    IF __rows = 0 THEN
      RAISE EXCEPTION '%: no rows, the physical order is untested', __table;
    END IF;
    IF __backward <> 0 THEN
      RAISE EXCEPTION '%: % of % rows lie on an earlier heap page than the row before them in (account, nai, balance_seq_no) order',
        __table, __backward, __rows;
    END IF;
    RAISE NOTICE 'PASS: % rows of % stored in (account, nai, balance_seq_no) order', __rows, __table;
  END LOOP;
END
$$;

DO
$$
DECLARE
  __table TEXT;
  __vacuumed BOOLEAN;
  __pages INT;
  __all_visible INT;
BEGIN
  FOREACH __table IN ARRAY ARRAY['account_balance_history', 'account_savings_history'] LOOP
    SELECT EXISTS (
      SELECT 1 FROM hafd.vacuum_requests r
      WHERE r.schema_name = current_setting('btracker_test.schema')
        AND r.table_name = __table
        AND to_jsonb(r) ->> 'kind' = 'freeze'
        AND r.status = 'vacuumed')
    INTO __vacuumed;

    IF NOT __vacuumed THEN
      RAISE NOTICE 'SKIP: % visibility map: HAF has not carried out a freeze request for it', __table;
      CONTINUE;
    END IF;

    SELECT relpages, relallvisible INTO __pages, __all_visible
    FROM pg_class WHERE oid = __table::regclass;
    IF __pages = 0 OR __all_visible < 0.95 * __pages THEN
      RAISE EXCEPTION '%: % of % pages all-visible after the freeze vacuum, expected at least 95%%',
        __table, __all_visible, __pages;
    END IF;
    RAISE NOTICE 'PASS: % of % pages of % all-visible', __all_visible, __pages, __table;
  END LOOP;
END
$$;
