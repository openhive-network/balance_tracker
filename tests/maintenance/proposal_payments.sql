/*
 * proposal_payments.sql
 * =====================
 * DHF proposal payments (process_block_range_proposal_payments) and the two
 * endpoints that read them, /proposals/payments and /proposals/{id}/payments:
 *
 *   - after the massive sync both tables are empty: the DHF starts at HF21, past
 *     the block range a test HAF holds;
 *   - synthetic proposal_pay_operations in the three blocks before the synced head:
 *     a two-block range processed as massive sync does, then the next block as a
 *     live block. Proposals 1 and 2 pay the same receiver, proposal 4 another one
 *     and proposal 3 is never paid. The totals, the payment numbering, the totals
 *     endpoint (with the unpaid proposal and a duplicate id) and the history
 *     endpoint's pages in both directions are checked against the operations;
 *   - after HAF rolls the context back (hive.context_back_from_fork) both tables
 *     are empty again (skipped when the context is not forking).
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Everything after the first check runs in a
 * transaction that is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- _btracker_ops_batch (what btracker_prefetch_operations gives the processing)
-- holding one proposal_pay_operation per element of _payments:
-- {"block", "pos", "proposal_id", "receiver", "amount"}.
CREATE FUNCTION pg_temp.stage_payments(_payments JSONB)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT hafd.operation_id((p ->> 'block')::INT, (p ->> 'pos')::INT) AS id,
         (p ->> 'block')::INT AS block_num, -1::SMALLINT AS trx_in_block, (p ->> 'pos')::INT AS op_pos,
         btracker_backend.op_proposal_pay()::SMALLINT AS op_type_id,
         jsonb_build_object(
           'proposal_id', (p ->> 'proposal_id')::INT,
           'receiver', p ->> 'receiver',
           'payer', p ->> 'receiver',
           'payment', jsonb_build_object('amount', p ->> 'amount', 'precision', 3, 'nai', '@@000000013')) AS body_value,
         NULL::INT AS custom_json_type_id
  FROM jsonb_array_elements(_payments) AS p;
END
$f$;

-- Pages of /proposals/{id}/payments as [[block_num, amount], ...].
CREATE FUNCTION pg_temp.history_page(_proposal_id INT, _page INT, _page_size INT, _direction TEXT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object(
    'total', h.total_operations,
    'pages', h.total_pages,
    'rows', COALESCE((SELECT jsonb_agg(jsonb_build_array(r.block_num, (r.amount).amount::BIGINT) ORDER BY o)
                      FROM unnest(h.operations_result) WITH ORDINALITY AS r(block_num, operation_id, amount, "timestamp", o)), '[]'))
  FROM btracker_endpoints.get_proposal_payments(_proposal_id, _page, _page_size, _direction::btracker_backend.sort_direction) h
$f$;

DO
$$
DECLARE
  __totals BIGINT := (SELECT count(*) FROM proposal_payment_totals);
  __payments BIGINT := (SELECT count(*) FROM proposal_payments);
BEGIN
  IF __totals <> 0 OR __payments <> 0 THEN
    RAISE EXCEPTION 'expected no proposal payments before HF21, found % totals and % payments', __totals, __payments;
  END IF;
  RAISE NOTICE 'PASS: no proposal payments after the massive sync';
END
$$;

BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __b3 CONSTANT INT := hive.app_get_current_block_num(__schema);
  __b2 CONSTANT INT := __b3 - 1;
  __b1 CONSTANT INT := __b3 - 2;
  __r TEXT;
  __s TEXT;
  __actual JSONB;
  __expected JSONB;
  __seq JSONB;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  SELECT av.name INTO __r FROM hive.accounts_view av ORDER BY av.id OFFSET 10 LIMIT 1;
  SELECT av.name INTO __s FROM hive.accounts_view av ORDER BY av.id OFFSET 11 LIMIT 1;
  IF __s IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts: % %', __r, __s;
  END IF;

  -- Operations past the blocks' own ones (op_pos from 10000), so their ids sort after them.
  PERFORM pg_temp.stage_payments(jsonb_build_array(
    jsonb_build_object('block', __b1, 'pos', 10001, 'proposal_id', 1, 'receiver', __r, 'amount', '1000'),
    jsonb_build_object('block', __b1, 'pos', 10002, 'proposal_id', 2, 'receiver', __r, 'amount', '2500'),
    jsonb_build_object('block', __b2, 'pos', 10001, 'proposal_id', 1, 'receiver', __r, 'amount', '1500'),
    jsonb_build_object('block', __b2, 'pos', 10002, 'proposal_id', 4, 'receiver', __s, 'amount', '7')));
  PERFORM process_block_range_proposal_payments(__b1, __b2);

  PERFORM pg_temp.stage_payments(jsonb_build_array(
    jsonb_build_object('block', __b3, 'pos', 10001, 'proposal_id', 2, 'receiver', __r, 'amount', '500'),
    jsonb_build_object('block', __b3, 'pos', 10002, 'proposal_id', 1, 'receiver', __r, 'amount', '1000')));
  PERFORM process_block_range_proposal_payments(__b3, __b3);

  -- Payment numbering: 1..n per proposal in operation order, across both calls.
  SELECT jsonb_object_agg(proposal_id, seqs) INTO __seq FROM (
    SELECT proposal_id, jsonb_agg(jsonb_build_array(payment_seq_no, hafd.operation_id_to_block_num(source_op), amount) ORDER BY payment_seq_no) AS seqs
    FROM proposal_payments GROUP BY proposal_id) s;
  __expected := jsonb_build_object(
    '1', jsonb_build_array(jsonb_build_array(1, __b1, 1000), jsonb_build_array(2, __b2, 1500), jsonb_build_array(3, __b3, 1000)),
    '2', jsonb_build_array(jsonb_build_array(1, __b1, 2500), jsonb_build_array(2, __b3, 500)),
    '4', jsonb_build_array(jsonb_build_array(1, __b2, 7)));
  IF __seq IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION 'proposal_payments: expected %, found %', __expected, __seq;
  END IF;
  RAISE NOTICE 'PASS: proposal_payments numbers each proposal''s payments across block ranges';

  -- The totals endpoint: distinct ids in ascending order, zero for the unpaid proposal 3.
  SELECT jsonb_agg(to_jsonb(t)) INTO __actual
  FROM btracker_endpoints.get_proposals_payments('4, 2,3,1,2') t;
  SELECT jsonb_agg(e ORDER BY (e ->> 'proposal_id')::INT) INTO __expected FROM (
    SELECT jsonb_build_object(
      'proposal_id', x.proposal_id, 'receiver', x.receiver,
      'total_paid', jsonb_build_object('nai', '@@000000013', 'amount', x.total, 'precision', 3),
      'payment_count', x.n,
      'first_payment_block', x.first_block, 'first_payment_time', to_jsonb(fb.created_at),
      'last_payment_block', x.last_block, 'last_payment_time', to_jsonb(lb.created_at)) AS e
    FROM (VALUES (1, __r, '3500', 3, __b1, __b3),
                 (2, __r, '3000', 2, __b1, __b3),
                 (3, NULL, '0', 0, NULL, NULL),
                 (4, __s, '7', 1, __b2, __b2)) AS x(proposal_id, receiver, total, n, first_block, last_block)
    LEFT JOIN hive.blocks_view fb ON fb.num = x.first_block
    LEFT JOIN hive.blocks_view lb ON lb.num = x.last_block) s;
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/proposals/payments: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: /proposals/payments sums each proposal of one receiver separately and zeroes the unpaid one';

  -- The history endpoint, page size 2 over proposal 1's three payments: page 1 is the
  -- oldest in both directions and the default page is the newest.
  __actual := jsonb_build_array(
    pg_temp.history_page(1, NULL, 2, 'desc'),
    pg_temp.history_page(1, 1, 2, 'desc'),
    pg_temp.history_page(1, 1, 2, 'asc'),
    pg_temp.history_page(1, 2, 2, 'asc'),
    pg_temp.history_page(3, NULL, 100, 'desc'));
  __expected := jsonb_build_array(
    jsonb_build_object('total', 3, 'pages', 2, 'rows', jsonb_build_array(jsonb_build_array(__b3, 1000))),
    jsonb_build_object('total', 3, 'pages', 2, 'rows', jsonb_build_array(jsonb_build_array(__b2, 1500), jsonb_build_array(__b1, 1000))),
    jsonb_build_object('total', 3, 'pages', 2, 'rows', jsonb_build_array(jsonb_build_array(__b1, 1000), jsonb_build_array(__b2, 1500))),
    jsonb_build_object('total', 3, 'pages', 2, 'rows', jsonb_build_array(jsonb_build_array(__b3, 1000))),
    jsonb_build_object('total', 0, 'pages', 0, 'rows', '[]'::JSONB));
  IF __actual IS DISTINCT FROM __expected THEN
    RAISE EXCEPTION '/proposals/{id}/payments: expected %, found %', __expected, __actual;
  END IF;
  RAISE NOTICE 'PASS: /proposals/{id}/payments pages a proposal''s payments in both directions';

  IF NOT (SELECT hc.is_forking FROM hafd.contexts hc WHERE hc.name = __schema) THEN
    RAISE NOTICE 'SKIP: fork rollback: context % is not forking', __schema;
    RETURN;
  END IF;

  -- The synthetic payments' changes were recorded at the context's current block.
  PERFORM hive.context_back_from_fork(__schema, __b3 - 1);

  IF EXISTS (SELECT 1 FROM proposal_payment_totals) OR EXISTS (SELECT 1 FROM proposal_payments) THEN
    RAISE EXCEPTION 'proposal payments survived the fork rollback';
  END IF;
  RAISE NOTICE 'PASS: a fork rollback reverts proposal_payment_totals and proposal_payments';
END
$$;
ROLLBACK;
