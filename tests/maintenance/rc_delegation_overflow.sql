/*
 * rc_delegation_overflow.sql
 * ==========================
 * hived's implicit reduction of RC delegations (resource_credits::
 * update_account_after_vest_change with the overflow check): after a
 * withdraw_vesting, a delegate_vesting_shares (delegator and delegatee) and a weekly
 * power-down payout, an account whose RC delegations exceed its effective vests
 *   vesting_shares - delegated_vesting_shares + received_vesting_shares
 *   - next_vesting_withdrawal
 * loses the excess from its delegations in delegatee account id order, at that
 * operation; and its one-off repair (repair_rc_delegation_overflow). The
 * delegated vests it counts drop to 0 at an account's HF23 clear, pending
 * delegation returns included, so a later payout cuts nothing hived keeps; and
 * the one-off repair of what earlier versions kept (hf_23_pending_delegation_returns).
 *
 * Each case is a synthetic block (operations past the head block's own, op_pos from
 * 10001) over a state set up in the btracker tables, processed in
 * btracker_single_processing's order. The test HAF predates HF26, where
 * process_block_range_rc_delegations returns, so the case calls the range function
 * it delegates to, process_rc_delegation_range, directly.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case runs in a transaction that is rolled
 * back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

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

CREATE FUNCTION pg_temp.withdraw_vesting(_account TEXT, _amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_withdraw_vesting(),
    'body', jsonb_build_object('account', _account, 'vesting_shares', pg_temp.vests(_amount)))
$f$;

CREATE FUNCTION pg_temp.fill_vesting_withdraw(_from TEXT, _to TEXT, _withdrawn BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_fill_vesting_withdraw(),
    'body', jsonb_build_object('from_account', _from, 'to_account', _to,
      'withdrawn', pg_temp.vests(_withdrawn), 'deposited', pg_temp.hive(_withdrawn / 2000)))
$f$;

CREATE FUNCTION pg_temp.delegate_vesting_shares(_delegator TEXT, _delegatee TEXT, _amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_delegate_vesting_shares(),
    'body', jsonb_build_object('delegator', _delegator, 'delegatee', _delegatee, 'vesting_shares', pg_temp.vests(_amount)))
$f$;

CREATE FUNCTION pg_temp.hardfork_hive(_account TEXT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_hardfork_hive(),
    'body', jsonb_build_object('account', _account, 'treasury', 'steem.dao'))
$f$;

CREATE FUNCTION pg_temp.delegate_rc(_from TEXT, _delegatees TEXT[], _max_rc BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_custom_json(),
    'body', jsonb_build_object('id', 'rc', 'required_auths', '[]'::JSONB, 'required_posting_auths', jsonb_build_array(_from),
      'json', jsonb_build_array('delegate_rc',
        jsonb_build_object('from', _from, 'delegatees', to_jsonb(_delegatees), 'max_rc', _max_rc, 'extensions', '[]'::JSONB))::TEXT))
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

CREATE FUNCTION pg_temp.op_id(_pos INT)
RETURNS BIGINT LANGUAGE sql
AS $f$
  SELECT hafd.operation_id(pg_temp.block(), 10000 + _pos)
$f$;

-- An account's vesting before the block: VESTS balance, power-down, delegated and
-- received vests. Its RC delegations start empty.
CREATE FUNCTION pg_temp.given(_account TEXT, _vests BIGINT, _rate BIGINT = 0, _to_withdraw BIGINT = 0, _withdrawn BIGINT = 0,
  _delegated_vests BIGINT = 0, _received_vests BIGINT = 0)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __id INT := pg_temp.account_id(_account);
BEGIN
  INSERT INTO current_account_balances (account, nai, balance, balance_change_count, source_op)
  VALUES (__id, btracker_backend.nai_vests(), _vests, 0, 0)
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO UPDATE SET balance = EXCLUDED.balance;

  INSERT INTO account_withdraws (account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op)
  VALUES (__id, _rate, _to_withdraw, _withdrawn, hafd.operation_id(pg_temp.block() - 1, 1))
  ON CONFLICT ON CONSTRAINT pk_account_withdraws DO UPDATE SET
    vesting_withdraw_rate = EXCLUDED.vesting_withdraw_rate, to_withdraw = EXCLUDED.to_withdraw,
    withdrawn = EXCLUDED.withdrawn, source_op = EXCLUDED.source_op;

  INSERT INTO account_delegations (account, delegated_vests, received_vests)
  VALUES (__id, _delegated_vests, _received_vests)
  ON CONFLICT ON CONSTRAINT pk_temp_vests DO UPDATE SET
    delegated_vests = EXCLUDED.delegated_vests, received_vests = EXCLUDED.received_vests;

  DELETE FROM current_rc_delegations WHERE delegator = __id;
  DELETE FROM account_rc_delegations WHERE account = __id;
END
$f$;

CREATE FUNCTION pg_temp.given_vesting_delegation(_delegator TEXT, _delegatee TEXT, _amount BIGINT)
RETURNS VOID LANGUAGE sql
AS $f$
  INSERT INTO current_accounts_delegations (delegator, delegatee, balance, source_op)
  VALUES (pg_temp.account_id(_delegator), pg_temp.account_id(_delegatee), _amount, 0)
$f$;

CREATE FUNCTION pg_temp.given_rc_delegation(_delegator TEXT, _delegatee TEXT, _max_rc BIGINT)
RETURNS VOID LANGUAGE sql
AS $f$
  INSERT INTO current_rc_delegations (delegator, delegatee, max_rc, source_op)
  VALUES (pg_temp.account_id(_delegator), pg_temp.account_id(_delegatee), _max_rc, 0);

  INSERT INTO account_rc_delegations AS ard (account, received_rc, delegated_rc)
  VALUES (pg_temp.account_id(_delegator), 0, _max_rc), (pg_temp.account_id(_delegatee), _max_rc, 0)
  ON CONFLICT ON CONSTRAINT pk_account_rc_delegations DO UPDATE SET
    received_rc = ard.received_rc + EXCLUDED.received_rc, delegated_rc = ard.delegated_rc + EXCLUDED.delegated_rc;
$f$;

-- One synthetic block of _ops (pg_temp.withdraw_vesting(...) etc., in order),
-- processed as btracker_single_processing does.
CREATE FUNCTION pg_temp.process_block(VARIADIC _ops JSONB[])
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __block CONSTANT INT := pg_temp.block();
BEGIN
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT pg_temp.op_id(o.pos::INT) AS id,
         __block AS block_num, -1::SMALLINT AS trx_in_block, (10000 + o.pos)::INT AS op_pos,
         (o.op->>'op')::SMALLINT AS op_type_id,
         o.op->'body' AS body_value,
         NULL::INT AS custom_json_type_id
  FROM unnest(_ops) WITH ORDINALITY AS o(op, pos);

  PERFORM btracker_backend.prefetch_account_clears(__block, __block, (SELECT count(*) FROM _btracker_ops_batch));
  PERFORM process_block_range_balances(__block, __block);
  PERFORM process_rc_delegation_range(__block, __block, __block);
  PERFORM process_block_range_withdrawals(__block, __block);
  PERFORM process_block_range_delegations(__block, __block);
END
$f$;

-- RC delegations of the given delegators: {"from>to": max_rc}
CREATE FUNCTION pg_temp.rc_delegations(VARIADIC _delegators TEXT[])
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_object_agg(dr.name || '>' || de.name, crd.max_rc), '{}')
  FROM current_rc_delegations crd
  JOIN hive.accounts_view dr ON dr.id = crd.delegator
  JOIN hive.accounts_view de ON de.id = crd.delegatee
  WHERE dr.name = ANY(_delegators)
$f$;

-- account_rc_delegations of the given accounts: {"account": [received_rc, delegated_rc]}
CREATE FUNCTION pg_temp.rc_totals(VARIADIC _accounts TEXT[])
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_object_agg(av.name, jsonb_build_array(ard.received_rc, ard.delegated_rc)), '{}')
  FROM account_rc_delegations ard
  JOIN hive.accounts_view av ON av.id = ard.account
  WHERE av.name = ANY(_accounts) AND (ard.received_rc <> 0 OR ard.delegated_rc <> 0)
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

-- The fixture: seven accounts in id order (a, b, c delegate RC; d is a vesting
-- delegation and power-down route counterparty; x1 < x2 < x3 by id receive RC),
-- and the weekly rate of a power-down of `amount` VESTS in the head block's
-- hardfork era (104 or 13 intervals; amount divides by both).
CREATE TEMP TABLE _fixture AS
SELECT
  n[1] AS a, n[2] AS b, n[3] AS c, n[4] AS d, n[5] AS x1, n[6] AS x2, n[7] AS x3,
  1352000000::BIGINT AS amount,
  1352000000::BIGINT / (CASE WHEN EXISTS (
    SELECT 1 FROM hafd.applied_hardforks
    WHERE hardfork_num = btracker_backend.hf_withdraw_rate() AND block_num < pg_temp.block()) THEN 13 ELSE 104 END) AS rate
FROM (SELECT array_agg(s.name ORDER BY s.id) AS n FROM (
  SELECT av.id, av.name::TEXT AS name FROM hive.accounts_view av ORDER BY av.id OFFSET 100 LIMIT 7) s) f;

-- Power-down start: a's effective vests drop by the weekly rate R below its RC
-- delegations (V - R/2): x1 (R/4) is removed, x2 (R/2) reduced to R/4, x3 untouched.
-- c powers down within its delegations: nothing changes.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v);
  PERFORM pg_temp.given(f.c, __v);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x3, __v - __r - __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.c, f.x1, __v - __r);

  PERFORM pg_temp.process_block(pg_temp.withdraw_vesting(f.a, f.amount), pg_temp.withdraw_vesting(f.c, f.amount));

  PERFORM pg_temp.expect('power-down start', 'RC delegations',
    pg_temp.rc_delegations(f.a, f.c),
    jsonb_build_object(f.a || '>' || f.x2, __r / 4, f.a || '>' || f.x3, __v - __r - __r / 4, f.c || '>' || f.x1, __v - __r));
  PERFORM pg_temp.expect('power-down start', 'account_rc_delegations',
    pg_temp.rc_totals(f.a, f.c, f.x1, f.x2, f.x3),
    jsonb_build_object(
      f.a, jsonb_build_array(0, __v - __r), f.c, jsonb_build_array(0, __v - __r),
      f.x1, jsonb_build_array(__v - __r, 0), f.x2, jsonb_build_array(__r / 4, 0), f.x3, jsonb_build_array(__v - __r - __r / 4, 0)));
  PERFORM pg_temp.expect('power-down start', 'source_op of the reduced delegation',
    to_jsonb((SELECT crd.source_op FROM current_rc_delegations crd
              WHERE crd.delegator = pg_temp.account_id(f.a) AND crd.delegatee = pg_temp.account_id(f.x2))),
    to_jsonb(pg_temp.op_id(1)));
  RAISE NOTICE 'PASS: a power-down start reduces RC delegations by the overflow in delegatee id order, at its operation; one within its vesting changes nothing';
END
$$;
ROLLBACK;

-- Weekly payout: a powers down R a week (withdrawn 11R of 13R so far), its RC
-- delegations equal its effective vests V - R. The payout (R/2 routed to d, R/2 to
-- itself) leaves V - R vests and the last withdrawal R ahead: effective V - 2R,
-- overflow R: x1 (R/2) removed, x2 (R) reduced to R/2.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v, __r, 13 * __r, 11 * __r);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __r);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x3, __v - __r - 3 * __r / 2);

  PERFORM pg_temp.process_block(pg_temp.fill_vesting_withdraw(f.a, f.d, __r / 2), pg_temp.fill_vesting_withdraw(f.a, f.a, __r / 2));

  PERFORM pg_temp.expect('weekly payout', 'RC delegations',
    pg_temp.rc_delegations(f.a),
    jsonb_build_object(f.a || '>' || f.x2, __r / 2, f.a || '>' || f.x3, __v - __r - 3 * __r / 2));
  PERFORM pg_temp.expect('weekly payout', 'account_rc_delegations',
    pg_temp.rc_totals(f.a, f.x1, f.x2, f.x3),
    jsonb_build_object(
      f.a, jsonb_build_array(0, __v - 2 * __r),
      f.x2, jsonb_build_array(__r / 2, 0), f.x3, jsonb_build_array(__v - __r - 3 * __r / 2, 0)));
  PERFORM pg_temp.expect('weekly payout', 'source_op of the reduced delegation',
    to_jsonb((SELECT crd.source_op FROM current_rc_delegations crd
              WHERE crd.delegator = pg_temp.account_id(f.a) AND crd.delegatee = pg_temp.account_id(f.x2))),
    to_jsonb(pg_temp.op_id(2)));
  RAISE NOTICE 'PASS: a weekly power-down payout reduces RC delegations by the overflow, after the payout''s last fill';
END
$$;
ROLLBACK;

-- Vesting delegation: a (RC delegations = V) delegates R/4, then R/2 VESTS to d:
-- overflow R/2, x1 (R/4) removed, x2 (R/2) reduced to R/4. b receives R VESTS from d and delegates
-- V + R RC to x1; d removes that delegation: overflow R on the delegatee side.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v);
  PERFORM pg_temp.given(f.b, __v, _received_vests => __r);
  PERFORM pg_temp.given(f.d, __v, _delegated_vests => __r);
  PERFORM pg_temp.given_vesting_delegation(f.d, f.b, __r);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x3, __v - 3 * __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.b, f.x1, __v + __r);

  PERFORM pg_temp.process_block(
    pg_temp.delegate_vesting_shares(f.a, f.d, __r / 4),
    pg_temp.delegate_vesting_shares(f.a, f.d, __r / 2),
    pg_temp.delegate_vesting_shares(f.d, f.b, 0));

  PERFORM pg_temp.expect('vesting delegation', 'RC delegations',
    pg_temp.rc_delegations(f.a, f.b),
    jsonb_build_object(f.a || '>' || f.x2, __r / 4, f.a || '>' || f.x3, __v - 3 * __r / 4, f.b || '>' || f.x1, __v));
  PERFORM pg_temp.expect('vesting delegation', 'account_rc_delegations',
    pg_temp.rc_totals(f.a, f.b, f.x1, f.x2, f.x3),
    jsonb_build_object(
      f.a, jsonb_build_array(0, __v - __r / 2), f.b, jsonb_build_array(0, __v),
      f.x1, jsonb_build_array(__v, 0), f.x2, jsonb_build_array(__r / 4, 0), f.x3, jsonb_build_array(__v - 3 * __r / 4, 0)));
  RAISE NOTICE 'PASS: a vesting delegation reduces the delegator''s and the delegatee''s RC delegations by their overflow';
END
$$;
ROLLBACK;

-- delegate_rc around a reduction in one block: a (RC delegations V - R/2) raises x3
-- by R/4, powers down (effective V - R, overflow 3R/4: x1 and x2 removed), then
-- lowers x3 to V - 2R and delegates R/2 to x1 again. c's delegate_rc, in a block
-- without a vest change of c, is squashed as before.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v);
  PERFORM pg_temp.given(f.c, __v);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x3, __v - __r - __r / 4);

  PERFORM pg_temp.process_block(
    pg_temp.delegate_rc(f.a, ARRAY[f.x3], __v - __r),
    pg_temp.withdraw_vesting(f.a, f.amount),
    pg_temp.delegate_rc(f.a, ARRAY[f.x3], __v - 2 * __r),
    pg_temp.delegate_rc(f.a, ARRAY[f.x1], __r / 2),
    pg_temp.delegate_rc(f.c, ARRAY[f.x2, f.x3], 7));

  PERFORM pg_temp.expect('delegate_rc after a reduction', 'RC delegations',
    pg_temp.rc_delegations(f.a, f.c),
    jsonb_build_object(f.a || '>' || f.x1, __r / 2, f.a || '>' || f.x3, __v - 2 * __r, f.c || '>' || f.x2, 7, f.c || '>' || f.x3, 7));
  PERFORM pg_temp.expect('delegate_rc after a reduction', 'account_rc_delegations',
    pg_temp.rc_totals(f.a, f.c, f.x1, f.x2, f.x3),
    jsonb_build_object(
      f.a, jsonb_build_array(0, __v - 3 * __r / 2), f.c, jsonb_build_array(0, 14),
      f.x1, jsonb_build_array(__r / 2, 0), f.x2, jsonb_build_array(7, 0), f.x3, jsonb_build_array(__v - 2 * __r + 7, 0)));
  RAISE NOTICE 'PASS: delegate_rc ops of a block apply in operation order around the reduction';
END
$$;
ROLLBACK;

-- The repair: delegations stored by a version without the reduction, whose
-- delegator is over its effective vests now (a: V - R/2 effective under a running
-- power-down with R/2 received, x1 R/4, x2 R/2 and x3 V - 3R/4 delegated: overflow
-- R/2), are reduced; c, within its vesting, is not.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
  __name CONSTANT TEXT := 'rc_delegation_overflow';
  __installed INT;
BEGIN
  SELECT rows_repaired INTO __installed FROM one_off_repairs WHERE name = __name;
  IF __installed IS NULL THEN
    RAISE EXCEPTION 'expected the install to record %', __name;
  END IF;

  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v, __r, 13 * __r, 0, _received_vests => __r / 2);
  PERFORM pg_temp.given(f.c, __v);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x3, __v - 3 * __r / 4);
  PERFORM pg_temp.given_rc_delegation(f.c, f.x1, __v);

  DELETE FROM one_off_repairs WHERE name = __name;
  PERFORM run_one_off_repairs();

  PERFORM pg_temp.expect('repair', 'RC delegations',
    pg_temp.rc_delegations(f.a, f.c),
    jsonb_build_object(f.a || '>' || f.x2, __r / 4, f.a || '>' || f.x3, __v - 3 * __r / 4, f.c || '>' || f.x1, __v));
  PERFORM pg_temp.expect('repair', 'account_rc_delegations',
    pg_temp.rc_totals(f.a, f.c, f.x1, f.x2, f.x3),
    jsonb_build_object(
      f.a, jsonb_build_array(0, __v - __r / 2), f.c, jsonb_build_array(0, __v),
      f.x1, jsonb_build_array(__v, 0), f.x2, jsonb_build_array(__r / 4, 0), f.x3, jsonb_build_array(__v - 3 * __r / 4, 0)));
  IF NOT EXISTS (SELECT 1 FROM one_off_repairs WHERE name = __name AND rows_repaired >= 2) THEN
    RAISE EXCEPTION 'expected % recorded with at least the 2 delegations it changed', __name;
  END IF;
  IF repair_rc_delegation_overflow() <> 0 THEN
    RAISE EXCEPTION 'repair_rc_delegation_overflow changed delegations of a repaired schema';
  END IF;
  RAISE NOTICE 'PASS: the repair reduces stored RC delegations over their delegator''s effective vests, once';
END
$$;
ROLLBACK;

-- HF23 clear, then a payout: a delegated R/2 VESTS to d and has R of an earlier
-- reduction still to come back (delegated vests 3R/2). Its HF23 clear removes the
-- delegation and the pending return: delegated vests 0. Later a powers down R a week
-- (withdrawn 11R of 13R) with RC delegations x1 R/2 and x2 V - 5R/2; the payout
-- leaves V - R vests and R ahead: effective V - 2R, their sum, so nothing is cut.
-- Counting the dropped return would cut R: x1 removed, x2 reduced.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __v BIGINT;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __v := f.amount; __r := f.rate;
  PERFORM pg_temp.given(f.a, __v, _delegated_vests => 3 * __r / 2);
  PERFORM pg_temp.given_vesting_delegation(f.a, f.d, __r / 2);
  PERFORM pg_temp.given(f.d, __v, _received_vests => __r / 2);

  PERFORM pg_temp.process_block(pg_temp.hardfork_hive(f.a));

  PERFORM pg_temp.expect('HF23 clear', 'delegated and received vests',
    (SELECT jsonb_build_array(ad.delegated_vests, ad.received_vests) FROM account_delegations ad WHERE ad.account = pg_temp.account_id(f.a)),
    jsonb_build_array(0, 0));
  PERFORM pg_temp.expect('HF23 clear', 'account_delegation_history at the clear',
    (SELECT jsonb_agg(jsonb_build_array(adh.delegated_vests, adh.received_vests))
     FROM account_delegation_history adh WHERE adh.account = pg_temp.account_id(f.a) AND adh.source_op = pg_temp.op_id(1)),
    jsonb_build_array(jsonb_build_array(0, 0)));
  PERFORM pg_temp.expect('HF23 clear', 'vesting delegations',
    to_jsonb((SELECT count(*) FROM current_accounts_delegations cad WHERE cad.delegator = pg_temp.account_id(f.a))),
    to_jsonb(0));

  UPDATE account_withdraws
  SET vesting_withdraw_rate = __r, to_withdraw = 13 * __r, withdrawn = 11 * __r
  WHERE account = pg_temp.account_id(f.a);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, __r / 2);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x2, __v - 5 * __r / 2);

  PERFORM pg_temp.process_block(pg_temp.fill_vesting_withdraw(f.a, f.a, __r));

  PERFORM pg_temp.expect('payout after an HF23 clear', 'RC delegations',
    pg_temp.rc_delegations(f.a),
    jsonb_build_object(f.a || '>' || f.x1, __r / 2, f.a || '>' || f.x2, __v - 5 * __r / 2));
  RAISE NOTICE 'PASS: an HF23 clear drops pending delegation returns, and a later payout within the vesting cuts no RC delegation';
END
$$;
ROLLBACK;

-- The repair of pending returns an earlier version kept past HF23, in a pretend HF23
-- block H (10 blocks below the head; the test HAF predates HF23): a session temp
-- view named operations_view adds the clears of a and b to the context's view.
-- a: delegated 3R/2 before H, R kept at its clear, then R/2 delegated (3R/2
-- stored); b: R kept with no row at its clear. a's rows from H on lose R and b gets
-- a row of 0 at its clear; once.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
  __name CONSTANT TEXT := 'hf_23_pending_delegation_returns';
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __h INT := pg_temp.block() - 10;
  __installed INT;
  __repaired INT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __r := f.rate;

  SELECT rows_repaired INTO __installed FROM one_off_repairs WHERE name = __name;
  IF __installed IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'expected the install to record % with 0 rows, found %', __name, __installed;
  END IF;

  PERFORM pg_temp.given(f.a, 0, _delegated_vests => 3 * __r / 2);
  PERFORM pg_temp.given(f.b, 0, _delegated_vests => __r, _received_vests => __r);
  DELETE FROM account_delegation_history WHERE account IN (pg_temp.account_id(f.a), pg_temp.account_id(f.b));
  INSERT INTO account_delegation_history (account, delegated_vests, received_vests, source_op)
  VALUES
    (pg_temp.account_id(f.a), 3 * __r / 2, 0, hafd.operation_id(__h - 5, 10001)),
    (pg_temp.account_id(f.a), __r, 0, hafd.operation_id(__h, 10001)),
    (pg_temp.account_id(f.a), 3 * __r / 2, 0, hafd.operation_id(__h + 2, 10001)),
    (pg_temp.account_id(f.b), __r, __r, hafd.operation_id(__h - 5, 10002));

  IF repair_hf_23_pending_delegation_returns() <> 0 THEN
    RAISE EXCEPTION 'repair_hf_23_pending_delegation_returns changed rows before the application processed HF23';
  END IF;

  EXECUTE format($sql$
    CREATE TEMP VIEW operations_view AS
    SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value FROM %1$I.operations_view ov
    UNION ALL
    SELECT hafd.operation_id(%2$s, 10000 + o.pos::INT), %2$s, btracker_backend.op_hardfork_hive()::SMALLINT,
           jsonb_build_object('account', o.name, 'treasury', 'steem.dao')
    FROM unnest(ARRAY[%3$L, %4$L]) WITH ORDINALITY AS o(name, pos)
  $sql$, __schema, __h, f.a, f.b);

  __repaired := drop_hf_23_pending_delegation_returns(__h);

  PERFORM pg_temp.expect('pending returns repair', 'delegated and received vests',
    (SELECT jsonb_object_agg(av.name, jsonb_build_array(ad.delegated_vests, ad.received_vests))
     FROM account_delegations ad JOIN hive.accounts_view av ON av.id = ad.account
     WHERE av.name IN (f.a, f.b)),
    jsonb_build_object(f.a, jsonb_build_array(__r / 2, 0), f.b, jsonb_build_array(0, __r)));
  PERFORM pg_temp.expect('pending returns repair', 'account_delegation_history',
    (SELECT jsonb_agg(jsonb_build_array(av.name, adh.source_op, adh.delegated_vests, adh.received_vests) ORDER BY av.name, adh.source_op)
     FROM account_delegation_history adh JOIN hive.accounts_view av ON av.id = adh.account
     WHERE av.name IN (f.a, f.b)),
    (SELECT jsonb_agg(e ORDER BY e->>0, (e->>1)::BIGINT) FROM (VALUES
      (jsonb_build_array(f.a, hafd.operation_id(__h - 5, 10001), 3 * __r / 2, 0)),
      (jsonb_build_array(f.a, hafd.operation_id(__h, 10001), 0, 0)),
      (jsonb_build_array(f.a, hafd.operation_id(__h + 2, 10001), __r / 2, 0)),
      (jsonb_build_array(f.b, hafd.operation_id(__h - 5, 10002), __r, __r)),
      (jsonb_build_array(f.b, hafd.operation_id(__h, 10002), 0, __r))) AS x(e)));
  IF __repaired <> 5 THEN
    RAISE EXCEPTION 'drop_hf_23_pending_delegation_returns: expected 5 rows changed, found %', __repaired;
  END IF;
  IF drop_hf_23_pending_delegation_returns(__h) <> 0 THEN
    RAISE EXCEPTION 'drop_hf_23_pending_delegation_returns changed rows of a repaired schema';
  END IF;
  RAISE NOTICE 'PASS: the repair drops the pending delegation returns kept past an HF23 clear from the totals and the later history, once';
END
$$;
ROLLBACK;
