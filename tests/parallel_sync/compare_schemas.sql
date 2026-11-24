-- Every table of the balance tracker in schema :serial against the same table in schema
-- :parallel, row for row (EXCEPT ALL both ways), without the surrogate hive_rowid HAF adds to
-- registered tables. HAF's own table named after the context (the schema) is not the balance
-- tracker's and is left out. Raises, naming each table that differs and a few of its rows, unless all
-- are equal and the two schemas hold the same tables.
--
--   psql -v serial=btracker_app -v parallel=hafbe_bal -f compare_schemas.sql

\set ON_ERROR_STOP on

SELECT set_config('btracker_test.serial', :'serial', false) AS serial_schema,
       set_config('btracker_test.parallel', :'parallel', false) AS parallel_schema \gset

DO $$
DECLARE
  __serial TEXT := current_setting('btracker_test.serial');
  __parallel TEXT := current_setting('btracker_test.parallel');
  -- Install bookkeeping (when each repair ran, finalize's own xid) rather than synced state, and
  -- the concurrent mode's progress rows and staging, which hold nothing once a sync is committed.
  __skip TEXT[] := ARRAY[
    'one_off_repairs', 'history_freeze_pending',
    'processor_group_progress', 'shared_batch_marker',
    'shared_batch_ops_0', 'shared_batch_ops_1', 'shared_batch_clears_0', 'shared_batch_clears_1'
  ];
  __unmatched TEXT;
  __table TEXT;
  __columns TEXT;
  __rows BIGINT;
  __missing BIGINT;
  __extra BIGINT;
  __sample TEXT;
  __differing TEXT[] := '{}';
BEGIN
  SELECT string_agg(t.relname || ' (' || t.side || ')', ', ' ORDER BY t.relname) INTO __unmatched
  FROM (
    (SELECT c.relname, 'only in ' || __serial AS side FROM pg_class c
     WHERE c.relnamespace = __serial::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> __serial
     EXCEPT
     SELECT c.relname, 'only in ' || __serial FROM pg_class c
     WHERE c.relnamespace = __parallel::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> __parallel)
    UNION ALL
    (SELECT c.relname, 'only in ' || __parallel FROM pg_class c
     WHERE c.relnamespace = __parallel::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> __parallel
     EXCEPT
     SELECT c.relname, 'only in ' || __parallel FROM pg_class c
     WHERE c.relnamespace = __serial::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> __serial)
  ) t;
  IF __unmatched IS NOT NULL THEN
    RAISE EXCEPTION 'the schemas hold different tables: %', __unmatched;
  END IF;

  FOR __table IN
    SELECT c.relname FROM pg_class c
    WHERE c.relnamespace = __serial::regnamespace AND c.relkind IN ('r', 'p') AND c.relname <> __serial
      AND c.relname <> ALL(__skip)
    ORDER BY c.relname
  LOOP
    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) INTO __columns
    FROM pg_attribute a
    WHERE a.attrelid = format('%I.%I', __serial, __table)::regclass
      AND a.attnum > 0 AND NOT a.attisdropped AND a.attname <> 'hive_rowid';

    EXECUTE format('SELECT count(*) FROM %I.%I', __serial, __table) INTO __rows;
    EXECUTE format('SELECT count(*) FROM (SELECT %1$s FROM %2$I.%4$I EXCEPT ALL SELECT %1$s FROM %3$I.%4$I) d',
                   __columns, __serial, __parallel, __table) INTO __missing;
    EXECUTE format('SELECT count(*) FROM (SELECT %1$s FROM %3$I.%4$I EXCEPT ALL SELECT %1$s FROM %2$I.%4$I) d',
                   __columns, __serial, __parallel, __table) INTO __extra;
    RAISE NOTICE '%: % rows in %, % of them missing from %, % extra rows there', __table, __rows, __serial, __missing, __parallel, __extra;

    IF __missing > 0 OR __extra > 0 THEN
      __differing := __differing || __table;
      EXECUTE format('SELECT string_agg(d::TEXT, E''\n'') FROM (SELECT %1$s FROM %2$I.%4$I EXCEPT ALL SELECT %1$s FROM %3$I.%4$I LIMIT 3) d',
                     __columns, __serial, __parallel, __table) INTO __sample;
      RAISE NOTICE '  only in %: %', __serial, __sample;
      EXECUTE format('SELECT string_agg(d::TEXT, E''\n'') FROM (SELECT %1$s FROM %3$I.%4$I EXCEPT ALL SELECT %1$s FROM %2$I.%4$I LIMIT 3) d',
                     __columns, __serial, __parallel, __table) INTO __sample;
      RAISE NOTICE '  only in %: %', __parallel, __sample;
    END IF;
  END LOOP;

  IF cardinality(__differing) > 0 THEN
    RAISE EXCEPTION 'tables differing between % and %: %', __serial, __parallel, array_to_string(__differing, ', ');
  END IF;
  RAISE NOTICE 'every table of % equals its copy in %', __serial, __parallel;
END
$$;
