/*
 * batch_temp_tables.sql
 * =====================
 * The per-batch temp tables keep their identity across live blocks (#118):
 *
 *   - btracker_prefetch_operations over several blocks (massive sync) creates
 *     _btracker_ops_batch and _btracker_clears_batch again on every call;
 *   - over a single block (live) it keeps both tables (same OID, so the cached
 *     plans reading them stay valid) and they hold exactly that block's rows;
 *   - btracker_reset_batch_table drops a kept table that grew over its page
 *     bound, and the next single-block prefetch creates it again.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced schema. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

CREATE FUNCTION pg_temp.expect_block(_block INT, _what TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __expected TEXT[];
  __actual TEXT[];
  __clears_expected BIGINT;
  __clears_actual BIGINT;
BEGIN
  SELECT array_agg(format('%s:%s:%s', o.id, o.op_type_id, md5(o.body_value::TEXT)) ORDER BY o.id) INTO __expected
  FROM operations_view o WHERE o.block_num = _block;
  SELECT array_agg(format('%s:%s:%s', b.id, b.op_type_id, md5(b.body_value::TEXT)) ORDER BY b.id) INTO __actual
  FROM _btracker_ops_batch b;
  IF __expected IS NULL OR __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '%: _btracker_ops_batch holds % rows, block % has % operations',
      _what, coalesce(cardinality(__actual), 0), _block, coalesce(cardinality(__expected), 0);
  END IF;

  SELECT count(*) INTO __clears_expected FROM _btracker_ops_batch b
  WHERE b.op_type_id IN (btracker_backend.op_clear_null_account_balance(), btracker_backend.op_consolidate_treasury_balance());
  SELECT count(*) INTO __clears_actual FROM _btracker_clears_batch c WHERE c.source_op_block = _block;
  IF __clears_actual <> __clears_expected OR EXISTS (SELECT 1 FROM _btracker_clears_batch c WHERE c.source_op_block <> _block) THEN
    RAISE EXCEPTION '%: _btracker_clears_batch holds % clears of block % (expected %) or rows of other blocks',
      _what, __clears_actual, _block, __clears_expected;
  END IF;
END
$f$;

BEGIN;
DO
$$
DECLARE
  __block CONSTANT INT := hive.app_get_current_block_num(current_setting('btracker_test.schema'));
  __ops_oid OID;
  __clears_oid OID;
BEGIN
  PERFORM btracker_prefetch_operations(__block - 1, __block);
  __ops_oid := to_regclass('pg_temp._btracker_ops_batch')::OID;
  __clears_oid := to_regclass('pg_temp._btracker_clears_batch')::OID;

  PERFORM btracker_prefetch_operations(__block - 1, __block);
  IF to_regclass('pg_temp._btracker_ops_batch')::OID = __ops_oid OR to_regclass('pg_temp._btracker_clears_batch')::OID = __clears_oid THEN
    RAISE EXCEPTION 'a multi-block prefetch kept the batch temp tables instead of creating them again';
  END IF;
  RAISE NOTICE 'PASS: a multi-block prefetch creates the batch temp tables again';

  __ops_oid := to_regclass('pg_temp._btracker_ops_batch')::OID;
  __clears_oid := to_regclass('pg_temp._btracker_clears_batch')::OID;
  PERFORM btracker_prefetch_operations(__block, __block);
  PERFORM pg_temp.expect_block(__block, 'first live block');
  PERFORM btracker_prefetch_operations(__block - 1, __block - 1);
  PERFORM pg_temp.expect_block(__block - 1, 'second live block');
  IF to_regclass('pg_temp._btracker_ops_batch')::OID <> __ops_oid OR to_regclass('pg_temp._btracker_clears_batch')::OID <> __clears_oid THEN
    RAISE EXCEPTION 'a single-block prefetch created the batch temp tables again instead of keeping them';
  END IF;
  RAISE NOTICE 'PASS: single-block prefetches keep the batch temp tables and hold only their block';

  INSERT INTO _btracker_ops_batch(id, block_num, body_value)
  SELECT -g, -1, to_jsonb((SELECT string_agg(md5(g::TEXT || i::TEXT), '') FROM generate_series(1, 50) i))
  FROM generate_series(1, 600) g;
  IF pg_relation_size('_btracker_ops_batch') <= 64 * 8192 THEN
    RAISE EXCEPTION 'the padding left _btracker_ops_batch at % pages, within the bound', pg_relation_size('_btracker_ops_batch') / 8192;
  END IF;
  PERFORM btracker_prefetch_operations(__block, __block);
  PERFORM pg_temp.expect_block(__block, 'live block after a grown table');
  IF to_regclass('pg_temp._btracker_ops_batch')::OID = __ops_oid THEN
    RAISE EXCEPTION 'a single-block prefetch kept a _btracker_ops_batch of % pages', pg_relation_size('_btracker_ops_batch') / 8192;
  END IF;
  RAISE NOTICE 'PASS: a single-block prefetch creates a grown _btracker_ops_batch again';
END
$$;
ROLLBACK;
