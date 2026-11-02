/*
 * delegation_withdraw_history.sql
 * ===============================
 * Balances at a past block and the delegation and power-down history behind them:
 *
 *   - after the sync, the latest account_delegation_history / account_withdraw_history
 *     row of every account equals account_delegations / account_withdraws, and the
 *     past-block reader at the last processed block gives every account the head
 *     answer's values;
 *   - synthetic blocks of delegations (delegate, re-delegate lower, undelegate, return)
 *     and power-downs (start, payouts, a payout of two fills, cancel, a payout that
 *     leaves 0 VESTS) write one history row per change, and the balances at each
 *     block, before and after every change, are the values replayed by hand below;
 *   - the upgrade backfill (backfill_delegation_and_withdraw_history) writes the same
 *     rows as block processing, over the synced chain plus the synthetic blocks;
 *   - a fork rollback removes the history rows of the rolled-back block;
 *   - the endpoints: at-block at the last processed block equals no at-block, a
 *     timestamp equals the block it resolves to, the fields without history are null
 *     at a past block, the batch endpoint answers in request order with each account's
 *     single-account answer, and the error cases name the problem.
 *
 * The synthetic operations go to the last four blocks before the head (op_pos from
 * 10001, after each block's own operations), for accounts with no power-down, no
 * delegation and no balance change from those blocks on, processed in
 * btracker_single_processing's order. The test HAF predates HF16: a power-down pays
 * 1/104 of its VESTS a week.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. Every case that writes runs in a transaction that
 * is rolled back. Run under `psql -v ON_ERROR_STOP=on`.
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

CREATE FUNCTION pg_temp.delegate_vesting_shares(_delegator TEXT, _delegatee TEXT, _amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_delegate_vesting_shares(),
    'body', jsonb_build_object('delegator', _delegator, 'delegatee', _delegatee, 'vesting_shares', pg_temp.vests(_amount)))
$f$;

CREATE FUNCTION pg_temp.return_vesting_delegation(_account TEXT, _amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object('op', btracker_backend.op_return_vesting_delegation(),
    'body', jsonb_build_object('account', _account, 'vesting_shares', pg_temp.vests(_amount)))
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

CREATE FUNCTION pg_temp.account_id(_name TEXT)
RETURNS INT LANGUAGE sql
AS $f$
  SELECT av.id FROM hive.accounts_view av WHERE av.name = _name
$f$;

CREATE FUNCTION pg_temp.head()
RETURNS INT LANGUAGE sql
AS $f$
  SELECT hive.app_get_current_block_num(current_setting('btracker_test.schema'))
$f$;

-- Every synthetic operation, for the backfill's view of the chain.
CREATE TEMP TABLE _synthetic_ops (id BIGINT, block_num INT, op_type_id SMALLINT, body_value JSONB);

-- One synthetic block of _ops (in order), processed as btracker_single_processing does.
CREATE FUNCTION pg_temp.process_block(_block INT, VARIADIC _ops JSONB[])
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT hafd.operation_id(_block, 10000 + o.pos::INT) AS id,
         _block AS block_num, -1::SMALLINT AS trx_in_block, (10000 + o.pos)::INT AS op_pos,
         (o.op->>'op')::SMALLINT AS op_type_id,
         o.op->'body' AS body_value,
         NULL::INT AS custom_json_type_id
  FROM unnest(_ops) WITH ORDINALITY AS o(op, pos);

  INSERT INTO _synthetic_ops SELECT id, block_num, op_type_id, body_value FROM _btracker_ops_batch;

  PERFORM process_block_range_balances(_block, _block);
  PERFORM process_rc_delegation_range(_block, _block, _block);
  PERFORM process_block_range_withdrawals(_block, _block);
  PERFORM process_block_range_delegations(_block, _block);
END
$f$;

-- An account's history-backed fields at _block:
-- [delegated_vests, received_vests, vesting_withdraw_rate, to_withdraw, withdrawn]
CREATE FUNCTION pg_temp.at(_account TEXT, _block INT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_array(b.delegated_vests::BIGINT, b.received_vests::BIGINT,
    b.vesting_withdraw_rate::BIGINT, b.to_withdraw::BIGINT, b.withdrawn::BIGINT)
  FROM btracker_backend.get_account_balances_at_block(pg_temp.account_id(_account), _block) b
$f$;

-- An account's history rows as [block, delegated, received] / [block, rate, to_withdraw, withdrawn].
CREATE FUNCTION pg_temp.delegation_rows(_account TEXT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_agg(jsonb_build_array(hafd.operation_id_to_block_num(h.source_op), h.delegated_vests, h.received_vests)
    ORDER BY h.source_op), '[]')
  FROM account_delegation_history h WHERE h.account = pg_temp.account_id(_account)
$f$;

CREATE FUNCTION pg_temp.withdraw_rows(_account TEXT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT COALESCE(jsonb_agg(jsonb_build_array(hafd.operation_id_to_block_num(h.source_op), h.vesting_withdraw_rate, h.to_withdraw, h.withdrawn)
    ORDER BY h.source_op), '[]')
  FROM account_withdraw_history h WHERE h.account = pg_temp.account_id(_account)
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

CREATE FUNCTION pg_temp.expect_error(_case TEXT, _sql TEXT, _message_like TEXT)
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  __message TEXT;
BEGIN
  BEGIN
    EXECUTE _sql;
  EXCEPTION WHEN raise_exception THEN
    GET STACKED DIAGNOSTICS __message = MESSAGE_TEXT;
    IF __message NOT LIKE _message_like THEN
      RAISE EXCEPTION '%: expected an error like %, found %', _case, _message_like, __message;
    END IF;
    RETURN;
  END;
  RAISE EXCEPTION '%: expected an error like %, the call succeeded', _case, _message_like;
END
$f$;

-- The fields the past-block reader resolves, and the ones it nulls.
CREATE FUNCTION pg_temp.history_fields(_b btracker_backend.balance)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_array(_b.hbd_balance, _b.hive_balance, _b.vesting_shares, _b.vesting_balance_hive,
    _b.post_voting_power_vests, _b.delegated_vests, _b.received_vests, _b.hbd_savings, _b.hive_savings,
    _b.vesting_withdraw_rate, _b.to_withdraw, _b.withdrawn)
$f$;

CREATE FUNCTION pg_temp.current_only_fields(_b btracker_backend.balance)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_array(_b.curation_rewards, _b.posting_rewards, _b.hbd_rewards, _b.hive_rewards, _b.vests_rewards,
    _b.hive_vesting_rewards, _b.savings_withdraw_requests, _b.withdraw_routes, _b.delayed_vests,
    _b.conversion_pending_amount_hbd, _b.conversion_pending_count_hbd, _b.conversion_pending_amount_hive,
    _b.conversion_pending_count_hive, _b.open_orders_hbd_count, _b.open_orders_hive_count, _b.open_orders_hive_amount,
    _b.open_orders_hbd_amount, _b.savings_pending_amount_hbd, _b.savings_pending_amount_hive,
    _b.escrow_pending_amount_hbd, _b.escrow_pending_amount_hive, _b.escrow_pending_count)
$f$;

-- After the sync: the latest history row of every account is its current state.
DO
$$
DECLARE
  __differing INT;
BEGIN
  IF NOT isIndexesCreated() THEN
    RAISE EXCEPTION 'expected a finalized schema';
  END IF;

  SELECT count(*) INTO __differing
  FROM account_withdraws aw
  FULL JOIN (SELECT DISTINCT ON (account) * FROM account_withdraw_history ORDER BY account, source_op DESC) h ON h.account = aw.account
  WHERE (COALESCE(aw.vesting_withdraw_rate, 0), COALESCE(aw.to_withdraw, 0), COALESCE(aw.withdrawn, 0))
    IS DISTINCT FROM (COALESCE(h.vesting_withdraw_rate, 0), COALESCE(h.to_withdraw, 0), COALESCE(h.withdrawn, 0));
  IF __differing <> 0 THEN
    RAISE EXCEPTION '% accounts'' latest account_withdraw_history row differs from account_withdraws', __differing;
  END IF;

  SELECT count(*) INTO __differing
  FROM account_delegations ad
  FULL JOIN (SELECT DISTINCT ON (account) * FROM account_delegation_history ORDER BY account, source_op DESC) h ON h.account = ad.account
  WHERE (COALESCE(ad.delegated_vests, 0), COALESCE(ad.received_vests, 0))
    IS DISTINCT FROM (COALESCE(h.delegated_vests, 0), COALESCE(h.received_vests, 0));
  IF __differing <> 0 THEN
    RAISE EXCEPTION '% accounts'' latest account_delegation_history row differs from account_delegations', __differing;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM account_withdraw_history) THEN
    RAISE EXCEPTION 'expected the sync to write account_withdraw_history';
  END IF;
  RAISE NOTICE 'PASS: the latest history row of every account equals its delegations and power-down';
END
$$;

-- The past-block reader at the last processed block, for every account.
DO
$$
DECLARE
  __head CONSTANT INT := pg_temp.head();
  __accounts INT;
  __differing INT;
BEGIN
  SELECT count(*), count(*) FILTER (WHERE pg_temp.history_fields(c.h) IS DISTINCT FROM pg_temp.history_fields(c.p))
  INTO __accounts, __differing
  FROM (
    SELECT btracker_backend.get_account_balances(a.account) AS h, btracker_backend.get_account_balances_at_block(a.account, __head) AS p
    FROM (SELECT DISTINCT account FROM current_account_balances) a
  ) c;
  IF __differing <> 0 THEN
    RAISE EXCEPTION 'the past-block reader at block % differs from the head answer for % of % accounts', __head, __differing, __accounts;
  END IF;
  RAISE NOTICE 'PASS: at the last processed block the past-block reader gives all % accounts their head values', __accounts;
END
$$;

-- The fixture: four accounts (a and c delegate to b; a and d power down) without a
-- power-down, a delegation or a balance change from four blocks before the head on,
-- and a weekly rate R.
CREATE TEMP TABLE _fixture AS
SELECT n[1] AS a, n[2] AS b, n[3] AS c, n[4] AS d, 1000000::BIGINT AS rate, pg_temp.head() - 4 AS b1
FROM (SELECT array_agg(s.name ORDER BY s.id) AS n FROM (
  SELECT av.id, av.name::TEXT AS name
  FROM hive.accounts_view av
  WHERE av.id > 1000
    AND NOT EXISTS (SELECT 1 FROM account_withdraws aw WHERE aw.account = av.id)
    AND NOT EXISTS (SELECT 1 FROM account_delegations ad WHERE ad.account = av.id)
    AND NOT EXISTS (
      SELECT 1 FROM account_balance_history abh
      WHERE abh.account = av.id AND hafd.operation_id_to_block_num(abh.source_op) >= pg_temp.head() - 4)
  ORDER BY av.id LIMIT 4) s) f;

-- The synthetic blocks b1..b4 (the head is b5):
--   b1: a delegates 1000 to b; a and d start powering down 104R
--   b2: a lowers its delegation to b to 600; c delegates 500 to b; a and d are paid R
--   b3: a undelegates b; 400 of a's delegation returns; a is paid R in two fills
--   b4: 600 return to a; a cancels its power-down; d is paid its last R VESTS
CREATE FUNCTION pg_temp.process_scenario()
RETURNS VOID LANGUAGE plpgsql
AS $f$
DECLARE
  f RECORD;
  __r BIGINT;
BEGIN
  SELECT * INTO f FROM _fixture;
  __r := f.rate;
  IF EXISTS (SELECT 1 FROM hafd.applied_hardforks WHERE hardfork_num = btracker_backend.hf_withdraw_rate() AND block_num <= f.b1) THEN
    RAISE EXCEPTION 'the scenario expects a chain before HF16 (104 weekly payouts)';
  END IF;

  INSERT INTO current_account_balances (account, nai, balance, balance_change_count, source_op)
  VALUES (pg_temp.account_id(f.a), btracker_backend.nai_vests(), 1000 * __r, 0, 0),
         (pg_temp.account_id(f.d), btracker_backend.nai_vests(), 2 * __r, 0, 0)
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO UPDATE SET balance = EXCLUDED.balance;

  PERFORM pg_temp.process_block(f.b1,
    pg_temp.delegate_vesting_shares(f.a, f.b, 1000),
    pg_temp.withdraw_vesting(f.a, 104 * __r),
    pg_temp.withdraw_vesting(f.d, 104 * __r));
  PERFORM pg_temp.process_block(f.b1 + 1,
    pg_temp.delegate_vesting_shares(f.a, f.b, 600),
    pg_temp.delegate_vesting_shares(f.c, f.b, 500),
    pg_temp.fill_vesting_withdraw(f.a, f.a, __r),
    pg_temp.fill_vesting_withdraw(f.d, f.d, __r));
  PERFORM pg_temp.process_block(f.b1 + 2,
    pg_temp.delegate_vesting_shares(f.a, f.b, 0),
    pg_temp.return_vesting_delegation(f.a, 400),
    pg_temp.fill_vesting_withdraw(f.a, f.d, __r / 2),
    pg_temp.fill_vesting_withdraw(f.a, f.a, __r / 2));
  PERFORM pg_temp.process_block(f.b1 + 3,
    pg_temp.return_vesting_delegation(f.a, 600),
    pg_temp.withdraw_vesting(f.a, 0),
    pg_temp.fill_vesting_withdraw(f.d, f.d, __r));
END
$f$;

BEGIN;
DO
$$
DECLARE
  f RECORD;
  __r BIGINT;
  __b INT;
  __synced_w INT;
  __synced_d INT;
  __differing INT;
BEGIN
  SELECT * INTO f FROM _fixture;
  IF f.d IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts';
  END IF;
  __r := f.rate;
  __b := f.b1;

  PERFORM pg_temp.process_scenario();

  -- One row per change; a lowered delegation changes only the delegatee until it returns.
  PERFORM pg_temp.expect('delegations', 'a''s history', pg_temp.delegation_rows(f.a),
    jsonb_build_array(jsonb_build_array(__b, 1000, 0), jsonb_build_array(__b + 2, 600, 0), jsonb_build_array(__b + 3, 0, 0)));
  PERFORM pg_temp.expect('delegations', 'b''s history', pg_temp.delegation_rows(f.b),
    jsonb_build_array(jsonb_build_array(__b, 0, 1000), jsonb_build_array(__b + 1, 0, 600), jsonb_build_array(__b + 1, 0, 1100),
                      jsonb_build_array(__b + 2, 0, 500)));
  PERFORM pg_temp.expect('delegations', 'c''s history', pg_temp.delegation_rows(f.c),
    jsonb_build_array(jsonb_build_array(__b + 1, 500, 0)));
  PERFORM pg_temp.expect('power-downs', 'a''s history', pg_temp.withdraw_rows(f.a),
    jsonb_build_array(jsonb_build_array(__b, __r, 104 * __r, 0), jsonb_build_array(__b + 1, __r, 104 * __r, __r),
                      jsonb_build_array(__b + 2, __r, 104 * __r, 3 * __r / 2), jsonb_build_array(__b + 2, __r, 104 * __r, 2 * __r),
                      jsonb_build_array(__b + 3, 0, 0, 0)));
  PERFORM pg_temp.expect('power-downs', 'd''s history', pg_temp.withdraw_rows(f.d),
    jsonb_build_array(jsonb_build_array(__b, __r, 104 * __r, 0), jsonb_build_array(__b + 1, __r, 104 * __r, __r),
                      jsonb_build_array(__b + 3, 0, 0, 0)));
  RAISE NOTICE 'PASS: delegations and power-downs write one history row per change';

  -- The state at each block: [delegated, received, rate, to_withdraw, withdrawn].
  PERFORM pg_temp.expect('before', 'a, b, c, d', jsonb_build_array(pg_temp.at(f.a, __b - 1), pg_temp.at(f.b, __b - 1), pg_temp.at(f.c, __b - 1), pg_temp.at(f.d, __b - 1)),
    jsonb_build_array('[0, 0, 0, 0, 0]'::JSONB, '[0, 0, 0, 0, 0]'::JSONB, '[0, 0, 0, 0, 0]'::JSONB, '[0, 0, 0, 0, 0]'::JSONB));
  PERFORM pg_temp.expect('b1: delegate, power-down start', 'a, b, c, d', jsonb_build_array(pg_temp.at(f.a, __b), pg_temp.at(f.b, __b), pg_temp.at(f.c, __b), pg_temp.at(f.d, __b)),
    jsonb_build_array(jsonb_build_array(1000, 0, __r, 104 * __r, 0), '[0, 1000, 0, 0, 0]'::JSONB, '[0, 0, 0, 0, 0]'::JSONB, jsonb_build_array(0, 0, __r, 104 * __r, 0)));
  PERFORM pg_temp.expect('b2: re-delegate, payout', 'a, b, c, d', jsonb_build_array(pg_temp.at(f.a, __b + 1), pg_temp.at(f.b, __b + 1), pg_temp.at(f.c, __b + 1), pg_temp.at(f.d, __b + 1)),
    jsonb_build_array(jsonb_build_array(1000, 0, __r, 104 * __r, __r), '[0, 1100, 0, 0, 0]'::JSONB, '[500, 0, 0, 0, 0]'::JSONB, jsonb_build_array(0, 0, __r, 104 * __r, __r)));
  PERFORM pg_temp.expect('b3: undelegate, return, two fills', 'a, b, c, d', jsonb_build_array(pg_temp.at(f.a, __b + 2), pg_temp.at(f.b, __b + 2), pg_temp.at(f.c, __b + 2), pg_temp.at(f.d, __b + 2)),
    jsonb_build_array(jsonb_build_array(600, 0, __r, 104 * __r, 2 * __r), '[0, 500, 0, 0, 0]'::JSONB, '[500, 0, 0, 0, 0]'::JSONB, jsonb_build_array(0, 0, __r, 104 * __r, __r)));
  PERFORM pg_temp.expect('b4: return, cancel, payout leaving 0 VESTS', 'a, b, c, d', jsonb_build_array(pg_temp.at(f.a, __b + 3), pg_temp.at(f.b, __b + 3), pg_temp.at(f.c, __b + 3), pg_temp.at(f.d, __b + 3)),
    jsonb_build_array('[0, 0, 0, 0, 0]'::JSONB, '[0, 500, 0, 0, 0]'::JSONB, '[500, 0, 0, 0, 0]'::JSONB, '[0, 0, 0, 0, 0]'::JSONB));
  PERFORM pg_temp.expect('head', 'a, b, c, d at the head',
    (SELECT jsonb_agg(jsonb_build_array(h.delegated_vests::BIGINT, h.received_vests::BIGINT, h.vesting_withdraw_rate::BIGINT, h.to_withdraw::BIGINT, h.withdrawn::BIGINT) ORDER BY n.pos)
     FROM unnest(ARRAY[f.a, f.b, f.c, f.d]) WITH ORDINALITY AS n(name, pos)
     CROSS JOIN LATERAL btracker_endpoints.get_account_balances(n.name) h),
    jsonb_build_array(pg_temp.at(f.a, __b + 3), pg_temp.at(f.b, __b + 3), pg_temp.at(f.c, __b + 3), pg_temp.at(f.d, __b + 3)));
  RAISE NOTICE 'PASS: the balances at every block before and after each change are the replayed ones';

  -- The upgrade backfill over the synced chain plus the synthetic blocks: the
  -- backfill reads operations_view, which this temporary view shadows.
  CREATE TEMP VIEW operations_view AS
  SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value FROM btracker_app.operations_view ov
  UNION ALL
  SELECT so.id, so.block_num, so.op_type_id, so.body_value FROM _synthetic_ops so;

  CREATE TEMP TABLE _synced_withdraws AS SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history;
  CREATE TEMP TABLE _synced_delegations AS SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history;
  SELECT count(*) INTO __synced_w FROM _synced_withdraws;
  SELECT count(*) INTO __synced_d FROM _synced_delegations;
  DELETE FROM account_withdraw_history;
  DELETE FROM account_delegation_history;

  PERFORM backfill_delegation_and_withdraw_history();

  SELECT count(*) INTO __differing FROM (
    (SELECT * FROM _synced_withdraws EXCEPT ALL SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history)
    UNION ALL
    (SELECT account, vesting_withdraw_rate, to_withdraw, withdrawn, source_op FROM account_withdraw_history EXCEPT ALL SELECT * FROM _synced_withdraws)) x;
  IF __differing <> 0 THEN
    RAISE EXCEPTION 'the backfill and block processing differ in % account_withdraw_history rows (of % processed)', __differing, __synced_w;
  END IF;
  SELECT count(*) INTO __differing FROM (
    (SELECT * FROM _synced_delegations EXCEPT ALL SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history)
    UNION ALL
    (SELECT account, delegated_vests, received_vests, source_op FROM account_delegation_history EXCEPT ALL SELECT * FROM _synced_delegations)) x;
  IF __differing <> 0 THEN
    RAISE EXCEPTION 'the backfill and block processing differ in % account_delegation_history rows (of % processed)', __differing, __synced_d;
  END IF;
  RAISE NOTICE 'PASS: the backfill writes the % power-down and % delegation history rows block processing wrote', __synced_w, __synced_d;
END
$$;
ROLLBACK;

-- A fork rollback removes the history rows written at the rolled-back block. Only
-- the synthetic blocks' rows are checked: the rollback below goes under the context's
-- irreversible block, which HAF never does, and so also reverts what an upgrade's
-- backfill wrote at that block.
BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __written BIGINT;
  __left BIGINT;
BEGIN
  IF NOT (SELECT hc.is_forking FROM hafd.contexts hc WHERE hc.name = __schema) THEN
    RAISE NOTICE 'SKIP: fork rollback: context % is not forking', __schema;
    RETURN;
  END IF;

  TRUNCATE _synthetic_ops;
  PERFORM pg_temp.process_scenario();
  __written := (SELECT count(*) FROM account_withdraw_history h WHERE h.source_op IN (SELECT id FROM _synthetic_ops))
             + (SELECT count(*) FROM account_delegation_history h WHERE h.source_op IN (SELECT id FROM _synthetic_ops));
  -- The synthetic blocks' changes were recorded at the context's current block.
  PERFORM hive.context_back_from_fork(__schema, pg_temp.head() - 1);

  __left := (SELECT count(*) FROM account_withdraw_history h WHERE h.source_op IN (SELECT id FROM _synthetic_ops))
          + (SELECT count(*) FROM account_delegation_history h WHERE h.source_op IN (SELECT id FROM _synthetic_ops));
  IF __written = 0 OR __left <> 0 THEN
    RAISE EXCEPTION 'a fork rollback left % of the % history rows of the rolled-back block', __left, __written;
  END IF;
  RAISE NOTICE 'PASS: a fork rollback removes the % history rows of the rolled-back block', __written;
END
$$;
ROLLBACK;

-- The endpoints.
DO
$$
DECLARE
  __head CONSTANT INT := pg_temp.head();
  __past CONSTANT INT := __head / 2;
  __accounts TEXT[];
  __timestamp TEXT;
  __past_answer btracker_backend.balance;
BEGIN
  -- The three accounts with the most balance changes.
  SELECT array_agg(av.name::TEXT ORDER BY t.changes DESC, av.name) INTO __accounts
  FROM (SELECT account, SUM(balance_change_count) AS changes FROM current_account_balances GROUP BY account ORDER BY 2 DESC LIMIT 3) t
  JOIN hive.accounts_view av ON av.id = t.account;

  PERFORM pg_temp.expect('at-block at the head', 'answer',
    (SELECT to_jsonb(b) FROM btracker_endpoints.get_account_balances(__accounts[1], __head::TEXT) b),
    (SELECT to_jsonb(b) FROM btracker_endpoints.get_account_balances(__accounts[1]) b));

  __past_answer := btracker_endpoints.get_account_balances(__accounts[1], __past::TEXT);
  PERFORM pg_temp.expect('at-block in the past', 'resolved fields',
    pg_temp.history_fields(__past_answer),
    pg_temp.history_fields(btracker_backend.get_account_balances_at_block(pg_temp.account_id(__accounts[1]), __past)));
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(pg_temp.current_only_fields(__past_answer)) e WHERE e <> 'null'::JSONB) THEN
    RAISE EXCEPTION 'at-block in the past: expected null for every field without history, found %', pg_temp.current_only_fields(__past_answer);
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(pg_temp.current_only_fields(btracker_endpoints.get_account_balances(__accounts[1]))) e WHERE e = 'null'::JSONB) THEN
    RAISE EXCEPTION 'without at-block: a field without history is null';
  END IF;

  SELECT to_char(bv.created_at, 'YYYY-MM-DD HH24:MI:SS') INTO __timestamp FROM hive.blocks_view bv WHERE bv.num = __past;
  PERFORM pg_temp.expect('at-block as a timestamp', 'answer',
    (SELECT to_jsonb(b) FROM btracker_endpoints.get_account_balances(__accounts[1], __timestamp) b),
    to_jsonb(__past_answer));
  RAISE NOTICE 'PASS: at-block at the head, in the past and as a timestamp';

  PERFORM pg_temp.expect('batch', 'answer',
    (SELECT jsonb_agg(to_jsonb(b)) FROM btracker_endpoints.get_accounts_balances(array_to_string(ARRAY[__accounts[3], __accounts[1], __accounts[2]], ','), __past::TEXT) b),
    (SELECT jsonb_agg(jsonb_build_object('account_name', n.name, 'balance', to_jsonb(btracker_endpoints.get_account_balances(n.name, __past::TEXT))) ORDER BY n.pos)
     FROM unnest(ARRAY[__accounts[3], __accounts[1], __accounts[2]]) WITH ORDINALITY AS n(name, pos)));
  PERFORM pg_temp.expect('batch at the head', 'answer',
    (SELECT jsonb_agg(to_jsonb(b)) FROM btracker_endpoints.get_accounts_balances(array_to_string(__accounts, ',')) b),
    (SELECT jsonb_agg(jsonb_build_object('account_name', n.name, 'balance', to_jsonb(btracker_endpoints.get_account_balances(n.name))) ORDER BY n.pos)
     FROM unnest(__accounts) WITH ORDINALITY AS n(name, pos)));
  RAISE NOTICE 'PASS: the batch answers in request order with each account''s single-account answer';

  PERFORM pg_temp.expect_error('at-block above the head',
    format('SELECT btracker_endpoints.get_account_balances(%L, %L)', __accounts[1], (__head + 1)::TEXT),
    format('at-block must be between block 1 and block %s %%', __head));
  PERFORM pg_temp.expect_error('at-block 0',
    format('SELECT btracker_endpoints.get_account_balances(%L, %L)', __accounts[1], '0'),
    'at-block must be between block 1 and block %');
  PERFORM pg_temp.expect_error('at-block before the first block',
    format('SELECT btracker_endpoints.get_account_balances(%L, %L)', __accounts[1], '2000-01-01 00:00:00'),
    'at-block must be between block 1 and block %');
  PERFORM pg_temp.expect_error('malformed at-block',
    format('SELECT btracker_endpoints.get_account_balances(%L, %L)', __accounts[1], 'last week'),
    'at-block must be a block number or a timestamp%');
  PERFORM pg_temp.expect_error('101 accounts',
    format('SELECT count(*) FROM btracker_endpoints.get_accounts_balances(%L)', array_to_string(array_fill(__accounts[1], ARRAY[101]), ',')),
    'accounts <= 100: accounts of 101 is greater than maximum allowed');
  PERFORM pg_temp.expect_error('unknown account in a batch',
    format('SELECT count(*) FROM btracker_endpoints.get_accounts_balances(%L)', __accounts[1] || ',no-such-account-x'),
    'Account ''no-such-account-x'' does not exist');
  RAISE NOTICE 'PASS: an out-of-range or malformed at-block, 101 accounts and an unknown account are errors naming the problem';
END
$$;
