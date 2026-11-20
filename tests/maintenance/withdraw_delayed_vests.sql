/*
 * withdraw_delayed_vests.sql
 * ==========================
 * hived takes a power-down payout from the withdrawer's delayed VESTS first: a payout
 * of W VESTS with D delayed erases min(W, D) of them and lowers the governance weight
 * by max(0, W - D) only (database_vesting.cpp process_vesting_withdrawals,
 * delayed_voting::update_votes). process_block_range_withdrawals records what each
 * fill_vesting_withdraw took in withdraw_delayed_vests:
 *
 *   - no row when the withdrawer has no delayed VESTS;
 *   - min(W, D) when it has, and account_withdraws.delayed_vests drops by it;
 *   - a payout split over routes: each fill takes from what the earlier ones left;
 *     a VESTS route credits its recipient's delayed VESTS, with no row for it;
 *   - a fill routing VESTS to its own withdrawer (auto_vest to itself) takes nothing
 *     and adds nothing: hived nets the route against the withdrawal. Both of its rows
 *     share a source_op; with the withdrawal ordered first, the clamp would empty
 *     the delayed VESTS and the deposit would refill them. As the withdrawer's last
 *     fill of the batch, the stored delayed VESTS are those after both rows;
 *   - a power-up (transfer_to_vesting_completed) or a maturing delayed_voting earlier
 *     in the block counts towards the D the fill sees.
 *
 * Each case is a synthetic block (operations past the head block's own, op_pos from
 * 10001) over a state set up in the btracker tables. The test HAF predates HF24
 * unless mock data simulated it: a case without HF24 before the head block records
 * it in hafd.applied_hardforks, inside the case's transaction.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case runs in a transaction that is rolled
 * back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

-- Without HF24 on the chain, the cases record it in hafd.applied_hardforks: a role
-- that may not (the AIDEV gate's consumer of the shared HAF, on the 5M-block chain)
-- skips the file.
SELECT EXISTS (SELECT 1 FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_delayed_voting())
       OR has_table_privilege('hafd.applied_hardforks', 'INSERT') AS can_run \gset
\if :can_run
\else
  \echo 'SKIP withdraw_delayed_vests.sql: no HF24 on this chain, and no INSERT on hafd.applied_hardforks to record it'
  \quit
\endif

CREATE FUNCTION pg_temp.vests(_amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('amount', _amount::TEXT, 'precision', 6, 'nai', '@@000000037')
$f$;

CREATE FUNCTION pg_temp.hive(_amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('amount', _amount::TEXT, 'precision', 3, 'nai', '@@000000021')
$f$;

-- A fill paying _withdrawn VESTS of _from to _to: as VESTS when _auto_vest (an
-- auto_vest route), else as HIVE.
CREATE FUNCTION pg_temp.fill(_from TEXT, _to TEXT, _withdrawn BIGINT, _auto_vest BOOLEAN = FALSE)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_fill_vesting_withdraw(),
    'body', jsonb_build_object('from_account', _from, 'to_account', _to,
      'withdrawn', pg_temp.vests(_withdrawn),
      'deposited', CASE WHEN _auto_vest THEN pg_temp.vests(_withdrawn) ELSE pg_temp.hive(_withdrawn / 2000) END))
$f$;

CREATE FUNCTION pg_temp.power_up(_to TEXT, _vests BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_transfer_to_vesting_completed(),
    'body', jsonb_build_object('from_account', _to, 'to_account', _to,
      'hive_vested', pg_temp.hive(_vests / 2000), 'vesting_shares_received', pg_temp.vests(_vests)))
$f$;

CREATE FUNCTION pg_temp.delayed_voting(_voter TEXT, _votes BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_delayed_voting(),
    'body', jsonb_build_object('voter', _voter, 'votes', _votes))
$f$;

CREATE FUNCTION pg_temp.account_id(_name TEXT)
RETURNS INT LANGUAGE sql
AS $f$
  SELECT av.id FROM hive.accounts_view av WHERE av.name = _name
$f$;

-- The block the synthetic operations go to: the synced head.
CREATE FUNCTION pg_temp.block()
RETURNS INT LANGUAGE sql
AS $f$
  SELECT COALESCE(NULLIF(hive.app_get_current_block_num(current_setting('btracker_test.schema')), 0), (SELECT MAX(num) FROM hafd.blocks))
$f$;

-- HF24 before the block, as process_block_range_withdrawals looks it up: recorded
-- in this transaction when the test HAF has not applied it, at the last operation
-- before the block (the table references the operation and its block).
CREATE FUNCTION pg_temp.with_hf24()
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __op BIGINT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_delayed_voting()) THEN
    SELECT o.id INTO __op FROM hafd.operations o
    WHERE o.id < hafd.operation_id(pg_temp.block(), 0)
    ORDER BY o.id DESC LIMIT 1;
    IF __op IS NULL THEN
      RAISE EXCEPTION 'no operation before block % to record HF24 at', pg_temp.block();
    END IF;
    INSERT INTO hafd.applied_hardforks (hardfork_num, block_num, hardfork_vop_id)
    VALUES (btracker_backend.hf_delayed_voting(), hafd.operation_id_to_block_num(__op), __op);
  END IF;
  IF (SELECT MAX(block_num) FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_delayed_voting()) >= pg_temp.block() THEN
    RAISE EXCEPTION 'HF24 is recorded at or after the head block %: no case block is past it', pg_temp.block();
  END IF;
END
$f$;

-- An account's delayed VESTS before the block.
CREATE FUNCTION pg_temp.given_delayed(_account TEXT, _delayed BIGINT)
RETURNS VOID LANGUAGE sql
AS $f$
  INSERT INTO account_withdraws (account, delayed_vests)
  VALUES (pg_temp.account_id(_account), _delayed)
  ON CONFLICT ON CONSTRAINT pk_account_withdraws DO UPDATE SET delayed_vests = EXCLUDED.delayed_vests
$f$;

-- One synthetic block of _ops, in order, through process_block_range_withdrawals
-- after the balances step it reads the batch's VESTS changes from.
CREATE FUNCTION pg_temp.process_block(VARIADIC _ops JSONB[])
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __block CONSTANT INT := pg_temp.block();
BEGIN
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT hafd.operation_id(__block, 10000 + o.pos::INT) AS id,
         __block AS block_num, -1::SMALLINT AS trx_in_block, (10000 + o.pos)::INT AS op_pos,
         (o.op->>'op')::SMALLINT AS op_type_id,
         o.op->'body' AS body_value,
         NULL::INT AS custom_json_type_id
  FROM unnest(_ops) WITH ORDINALITY AS o(op, pos);

  PERFORM btracker_backend.prefetch_account_clears(__block, __block, (SELECT count(*) FROM _btracker_ops_batch));
  PERFORM process_block_range_balances(__block, __block);
  PERFORM process_block_range_withdrawals(__block, __block);
END
$f$;

-- The given accounts' delayed VESTS: {"account": delayed_vests}
CREATE FUNCTION pg_temp.delayed(VARIADIC _accounts TEXT[])
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_object_agg(av.name, aw.delayed_vests), '{}')
  FROM account_withdraws aw
  JOIN hive.accounts_view av ON av.id = aw.account
  WHERE av.name = ANY(_accounts)
$f$;

-- The rows recorded for the block: [[account, op_pos, delayed_vests], ...]
CREATE FUNCTION pg_temp.taken()
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_agg(jsonb_build_array(av.name, t.source_op - hafd.operation_id(pg_temp.block(), 10000), t.delayed_vests)
                            ORDER BY t.source_op, av.name), '[]')
  FROM withdraw_delayed_vests t
  JOIN hive.accounts_view av ON av.id = t.account
  WHERE hafd.operation_id_to_block_num(t.source_op) = pg_temp.block()
$f$;

CREATE FUNCTION pg_temp.expect(_case TEXT, _what TEXT, _actual JSONB, _expected JSONB)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  IF _actual IS DISTINCT FROM _expected THEN
    RAISE EXCEPTION '%: % expected %, found %', _case, _what, _expected, _actual;
  END IF;
END
$f$;

-- Four accounts in id order (a, b, c withdraw; d is a route recipient) and a payout R.
CREATE TEMP TABLE _fixture AS
SELECT n[1] AS a, n[2] AS b, n[3] AS c, n[4] AS d, 12000000::BIGINT AS r
FROM (SELECT array_agg(s.name ORDER BY s.id) AS n FROM (
  SELECT av.id, av.name::TEXT AS name FROM hive.accounts_view av ORDER BY av.id OFFSET 100 LIMIT 4) s) f;

DO $$
BEGIN
  IF (SELECT d FROM _fixture) IS NULL THEN
    RAISE EXCEPTION 'withdraw_delayed_vests: the test HAF has fewer than 104 accounts';
  END IF;
END
$$;

-- The table is registered with HAF (its rows are reverted on a fork like the other
-- registered tables'; the revert itself is not exercised here).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM hafd.registered_tables rt
    WHERE rt.origin_table_schema = current_setting('btracker_test.schema')
      AND rt.origin_table_name = 'withdraw_delayed_vests') THEN
    RAISE EXCEPTION 'withdraw_delayed_vests is not registered with context %', current_setting('btracker_test.schema');
  END IF;
  RAISE NOTICE 'PASS: withdraw_delayed_vests is registered with the context';
END
$$;

-- No delayed VESTS (a), more than the payout (b), less than the payout (c): one
-- payout of R each, to the withdrawer as HIVE.
BEGIN;
DO $$
DECLARE
  f RECORD;
BEGIN
  SELECT * INTO f FROM _fixture;
  PERFORM pg_temp.with_hf24();
  PERFORM pg_temp.given_delayed(f.a, 0);
  PERFORM pg_temp.given_delayed(f.b, 3 * f.r);
  PERFORM pg_temp.given_delayed(f.c, f.r / 4);

  PERFORM pg_temp.process_block(
    pg_temp.fill(f.a, f.a, f.r),
    pg_temp.fill(f.b, f.b, f.r),
    pg_temp.fill(f.c, f.c, f.r));

  PERFORM pg_temp.expect('one payout each', 'rows', pg_temp.taken(),
    jsonb_build_array(jsonb_build_array(f.b, 2, f.r), jsonb_build_array(f.c, 3, f.r / 4)));
  PERFORM pg_temp.expect('one payout each', 'delayed VESTS', pg_temp.delayed(f.a, f.b, f.c),
    jsonb_build_object(f.a, 0, f.b, 2 * f.r, f.c, 0));
  RAISE NOTICE 'PASS: a payout takes min(W, D) from the delayed VESTS, recorded only when it takes some';
END
$$;
ROLLBACK;

-- A payout split over routes: a has R/4 delayed and pays R/2 to d as VESTS (auto_vest),
-- then R/2 to itself as HIVE. The first fill takes all R/4, the second nothing; d,
-- without an account_withdraws row before, gets R/2 delayed VESTS and no row here.
BEGIN;
DO $$
DECLARE
  f RECORD;
BEGIN
  SELECT * INTO f FROM _fixture;
  PERFORM pg_temp.with_hf24();
  PERFORM pg_temp.given_delayed(f.a, f.r / 4);
  DELETE FROM account_withdraws WHERE account = pg_temp.account_id(f.d);

  PERFORM pg_temp.process_block(
    pg_temp.fill(f.a, f.d, f.r / 2, TRUE),
    pg_temp.fill(f.a, f.a, f.r / 2));

  PERFORM pg_temp.expect('routed payout', 'rows', pg_temp.taken(),
    jsonb_build_array(jsonb_build_array(f.a, 1, f.r / 4)));
  PERFORM pg_temp.expect('routed payout', 'delayed VESTS', pg_temp.delayed(f.a, f.d),
    jsonb_build_object(f.a, 0, f.d, f.r / 2));
  RAISE NOTICE 'PASS: the fills of one payout take from what the earlier ones left; a VESTS route credits its recipient';
END
$$;
ROLLBACK;

-- A fill routing VESTS to its own withdrawer, without delayed VESTS (a) and with R (b):
-- R/2 to itself as VESTS (auto_vest), then R/2 as HIVE. The route takes and adds
-- nothing; b's HIVE fill takes R/2.
BEGIN;
DO $$
DECLARE
  f RECORD;
BEGIN
  SELECT * INTO f FROM _fixture;
  PERFORM pg_temp.with_hf24();
  PERFORM pg_temp.given_delayed(f.a, 0);
  PERFORM pg_temp.given_delayed(f.b, f.r);

  PERFORM pg_temp.process_block(
    pg_temp.fill(f.a, f.a, f.r / 2, TRUE),
    pg_temp.fill(f.a, f.a, f.r / 2),
    pg_temp.fill(f.b, f.b, f.r / 2, TRUE),
    pg_temp.fill(f.b, f.b, f.r / 2));

  PERFORM pg_temp.expect('route to itself', 'rows', pg_temp.taken(),
    jsonb_build_array(jsonb_build_array(f.b, 4, f.r / 2)));
  PERFORM pg_temp.expect('route to itself', 'delayed VESTS', pg_temp.delayed(f.a, f.b),
    jsonb_build_object(f.a, 0, f.b, f.r / 2));
  RAISE NOTICE 'PASS: a VESTS route to the withdrawer itself neither takes nor adds delayed VESTS';
END
$$;
ROLLBACK;

-- The same route as the withdrawer's last fill of the batch: c (R delayed) routes R/2
-- to itself as VESTS and nothing follows. The stored delayed VESTS are the value after
-- both of the fill's rows, R, not the R + R/2 between them.
BEGIN;
DO $$
DECLARE
  f RECORD;
BEGIN
  SELECT * INTO f FROM _fixture;
  PERFORM pg_temp.with_hf24();
  PERFORM pg_temp.given_delayed(f.c, f.r);

  PERFORM pg_temp.process_block(pg_temp.fill(f.c, f.c, f.r / 2, TRUE));

  PERFORM pg_temp.expect('route to itself last', 'rows', pg_temp.taken(), '[]');
  PERFORM pg_temp.expect('route to itself last', 'delayed VESTS', pg_temp.delayed(f.c),
    jsonb_build_object(f.c, f.r));
  RAISE NOTICE 'PASS: a VESTS route to the withdrawer itself as its last fill leaves the stored delayed VESTS as they were';
END
$$;
ROLLBACK;

-- Earlier in the block: a powers up R (delayed) before a payout of 2R; b's R delayed
-- mature (delayed_voting) before a payout of R. a's fill takes R, b's nothing.
BEGIN;
DO $$
DECLARE
  f RECORD;
BEGIN
  SELECT * INTO f FROM _fixture;
  PERFORM pg_temp.with_hf24();
  PERFORM pg_temp.given_delayed(f.a, 0);
  PERFORM pg_temp.given_delayed(f.b, f.r);

  PERFORM pg_temp.process_block(
    pg_temp.power_up(f.a, f.r),
    pg_temp.delayed_voting(f.b, f.r),
    pg_temp.fill(f.a, f.a, 2 * f.r),
    pg_temp.fill(f.b, f.b, f.r));

  PERFORM pg_temp.expect('same-block changes', 'rows', pg_temp.taken(),
    jsonb_build_array(jsonb_build_array(f.a, 3, f.r)));
  PERFORM pg_temp.expect('same-block changes', 'delayed VESTS', pg_temp.delayed(f.a, f.b),
    jsonb_build_object(f.a, 0, f.b, 0));
  RAISE NOTICE 'PASS: a power-up or maturing delayed VESTS earlier in the block count towards what a fill takes';
END
$$;
ROLLBACK;
