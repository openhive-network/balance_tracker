/*
 * power_down_without_vests.sql
 * ============================
 * hived's process_vesting_withdrawals ends a power-down (vesting_withdraw_rate,
 * to_withdraw and withdrawn reset to 0) at a weekly payout that leaves the account
 * with 0 VESTS:
 *
 *   - process_block_range_withdrawals ends it at that payout; a payout that leaves
 *     VESTS keeps the power-down running;
 *   - the RC delegation overflow checks at and after that payout use a next
 *     withdrawal of 0;
 *   - the one-off repair (power_downs_without_vests) ends the power-down of every
 *     account with 0 VESTS, once, and leaves the others.
 *
 * Each processing case is a synthetic block (operations past the head block's own,
 * op_pos from 10001) over a state set up in the btracker tables, processed in
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

-- One synthetic block of _ops (pg_temp.fill_vesting_withdraw(...) etc., in order),
-- processed as btracker_single_processing does.
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

  PERFORM process_block_range_balances(__block, __block);
  PERFORM process_rc_delegation_range(__block, __block, __block);
  PERFORM process_block_range_withdrawals(__block, __block);
  PERFORM process_block_range_delegations(__block, __block);
END
$f$;

-- Power-down state of the given accounts: {"account": [rate, to_withdraw, withdrawn]}
CREATE FUNCTION pg_temp.power_downs(VARIADIC _accounts TEXT[])
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_object_agg(av.name, jsonb_build_array(aw.vesting_withdraw_rate, aw.to_withdraw, aw.withdrawn)), '{}')
  FROM account_withdraws aw
  JOIN hive.accounts_view av ON av.id = aw.account
  WHERE av.name = ANY(_accounts)
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

CREATE FUNCTION pg_temp.expect(_case TEXT, _what TEXT, _actual JSONB, _expected JSONB)
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  IF _actual IS DISTINCT FROM _expected THEN
    RAISE EXCEPTION '%: % expected %, found %', _case, _what, _expected, _actual;
  END IF;
END
$f$;

-- The fixture: five accounts in id order (a, b power down; c has neither VESTS nor
-- a power-down; d is the payout route and vesting delegation counterparty; x1
-- receives RC) and a weekly rate R.
CREATE TEMP TABLE _fixture AS
SELECT n[1] AS a, n[2] AS b, n[3] AS c, n[4] AS d, n[5] AS x1, 13000000::BIGINT AS rate
FROM (SELECT array_agg(s.name ORDER BY s.id) AS n FROM (
  SELECT av.id, av.name::TEXT AS name FROM hive.accounts_view av ORDER BY av.id OFFSET 100 LIMIT 5) s) f;

-- Payouts: a (R VESTS, 11R of 13R withdrawn) is paid R in two fills (R/2 to d, R/2
-- to itself as HIVE), leaving 0 VESTS: its power-down ends. b (2R VESTS, same
-- power-down) is paid R, leaving R VESTS: its power-down continues at 12R withdrawn.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __r := f.rate;
  PERFORM pg_temp.given(f.a, __r, __r, 13 * __r, 11 * __r);
  PERFORM pg_temp.given(f.b, 2 * __r, __r, 13 * __r, 11 * __r);

  PERFORM pg_temp.process_block(
    pg_temp.fill_vesting_withdraw(f.a, f.d, __r / 2),
    pg_temp.fill_vesting_withdraw(f.a, f.a, __r / 2),
    pg_temp.fill_vesting_withdraw(f.b, f.b, __r));

  PERFORM pg_temp.expect('payouts', 'power-downs',
    pg_temp.power_downs(f.a, f.b),
    jsonb_build_object(f.a, jsonb_build_array(0, 0, 0), f.b, jsonb_build_array(__r, 13 * __r, 12 * __r)));
  RAISE NOTICE 'PASS: a payout that leaves 0 VESTS ends the power-down; one that leaves VESTS keeps it running';
END
$$;
ROLLBACK;

-- RC overflow at the payout: a (R VESTS, 11R of 13R withdrawn, 2R received from d)
-- delegates 2R RC to x1. The payout of R leaves 0 VESTS and ends the power-down:
-- effective vests 2R, no overflow (with the power-down still running, R and x1 cut
-- to R).
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __r := f.rate;
  PERFORM pg_temp.given(f.a, __r, __r, 13 * __r, 11 * __r, _received_vests => 2 * __r);
  PERFORM pg_temp.given(f.d, 4 * __r, _delegated_vests => 2 * __r);
  PERFORM pg_temp.given_vesting_delegation(f.d, f.a, 2 * __r);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, 2 * __r);

  PERFORM pg_temp.process_block(pg_temp.fill_vesting_withdraw(f.a, f.d, __r));

  PERFORM pg_temp.expect('RC overflow at the payout', 'RC delegations',
    pg_temp.rc_delegations(f.a), jsonb_build_object(f.a || '>' || f.x1, 2 * __r));
  RAISE NOTICE 'PASS: the RC overflow check at a payout that leaves 0 VESTS sees no next withdrawal';
END
$$;
ROLLBACK;

-- RC overflow after the payout, in its batch: the same payout, then d lowers its
-- vesting delegation to a to R: effective vests R, overflow R, x1 reduced to R (with
-- the power-down still running, 0 and x1 removed). The withdrawals step stores the
-- ended power-down only after the RC step has replayed the batch.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __r := f.rate;
  PERFORM pg_temp.given(f.a, __r, __r, 13 * __r, 11 * __r, _received_vests => 2 * __r);
  PERFORM pg_temp.given(f.d, 4 * __r, _delegated_vests => 2 * __r);
  PERFORM pg_temp.given_vesting_delegation(f.d, f.a, 2 * __r);
  PERFORM pg_temp.given_rc_delegation(f.a, f.x1, 2 * __r);

  PERFORM pg_temp.process_block(
    pg_temp.fill_vesting_withdraw(f.a, f.d, __r),
    pg_temp.delegate_vesting_shares(f.d, f.a, __r));

  PERFORM pg_temp.expect('RC overflow in one batch', 'RC delegations',
    pg_temp.rc_delegations(f.a), jsonb_build_object(f.a || '>' || f.x1, __r));
  PERFORM pg_temp.expect('RC overflow in one batch', 'power-downs',
    pg_temp.power_downs(f.a), jsonb_build_object(f.a, jsonb_build_array(0, 0, 0)));
  RAISE NOTICE 'PASS: a later check in the batch of a payout that leaves 0 VESTS sees no next withdrawal';
END
$$;
ROLLBACK;

-- The repair: a power-down stored by a version that kept it after a payout left
-- 0 VESTS (a) is ended; b's, with VESTS, and c's state are left as they are.
BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
  __name CONSTANT TEXT := 'power_downs_without_vests';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM one_off_repairs WHERE name = __name) THEN
    RAISE EXCEPTION 'expected the install to record %', __name;
  END IF;

  SELECT * INTO f FROM _fixture;
  __r := f.rate;
  PERFORM pg_temp.given(f.a, 0, __r, 13 * __r, 12 * __r);
  PERFORM pg_temp.given(f.b, 2 * __r, __r, 13 * __r, 11 * __r);
  PERFORM pg_temp.given(f.c, 0);

  DELETE FROM one_off_repairs WHERE name = __name;
  PERFORM run_one_off_repairs();

  PERFORM pg_temp.expect('repair', 'power-downs',
    pg_temp.power_downs(f.a, f.b, f.c),
    jsonb_build_object(
      f.a, jsonb_build_array(0, 0, 0), f.b, jsonb_build_array(__r, 13 * __r, 11 * __r), f.c, jsonb_build_array(0, 0, 0)));
  IF NOT EXISTS (SELECT 1 FROM one_off_repairs WHERE name = __name AND rows_repaired >= 1) THEN
    RAISE EXCEPTION 'expected % recorded with at least the power-down it ended', __name;
  END IF;
  IF repair_power_downs_without_vests() <> 0 THEN
    RAISE EXCEPTION 'repair_power_downs_without_vests changed power-downs of a repaired schema';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM account_withdraws aw
    LEFT JOIN current_account_balances cab ON cab.account = aw.account AND cab.nai = btracker_backend.nai_vests()
    WHERE aw.to_withdraw > aw.withdrawn AND COALESCE(cab.balance, 0) = 0
  ) THEN
    RAISE EXCEPTION 'an account with 0 VESTS keeps an active power-down after the repair';
  END IF;
  RAISE NOTICE 'PASS: the repair ends the power-downs of accounts with 0 VESTS, once';
END
$$;
ROLLBACK;
