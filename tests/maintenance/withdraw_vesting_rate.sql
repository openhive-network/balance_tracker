/*
 * withdraw_vesting_rate.sql
 * =========================
 * The weekly power-down rate (account_withdraws.vesting_withdraw_rate), which must
 * equal hived's withdraw_vesting_evaluator, and its one-off repair:
 *
 *   - process_withdraw_vesting_operation under each hardfork era: round down with
 *     104 intervals pre-HF16 and 13 after, round up from HF21 on, a minimum of 1 for
 *     a non-zero amount, 0 for a cancel;
 *   - a synthetic withdraw_vesting in the synced head block, processed by
 *     process_block_range_withdrawals: the rate of the hardfork era the test HAF's
 *     head is in, and the minimum of 1;
 *   - the repair (repair_vesting_withdraw_rate, applied by run_one_off_repairs): the
 *     install recorded it with nothing to change; power-downs whose stored rate is
 *     one below their withdraw_vesting's get it back, the run is recorded, every
 *     other row is unchanged and run_one_off_repairs does not apply it twice.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. The processing and repair checks run in
 * transactions that are rolled back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

CREATE FUNCTION pg_temp.withdraw_vesting_body(_account TEXT, _amount BIGINT)
RETURNS JSONB LANGUAGE sql
AS $f$
  SELECT jsonb_build_object(
    'account', _account,
    'vesting_shares', jsonb_build_object('amount', _amount::TEXT, 'precision', 6, 'nai', '@@000000037'))
$f$;

DO
$$
DECLARE
  __case RECORD;
  __actual BIGINT;
  __failures TEXT[] := '{}';
BEGIN
  FOR __case IN
    SELECT * FROM (VALUES
      ('post-HF21, remainder: rounded up',          1000000000001, TRUE,  TRUE,  TRUE,  76923076924),
      ('post-HF21, divisible by 13: exact',          1300000000000, TRUE,  TRUE,  TRUE,  100000000000),
      ('post-HF21, 14 VESTS: rounded up to 2',       14,            TRUE,  TRUE,  TRUE,  2),
      ('post-HF21, 12 VESTS: minimum of 1',          12,            TRUE,  TRUE,  TRUE,  1),
      ('post-HF16 pre-HF21: rounded down',           1000000000001, TRUE,  TRUE,  FALSE, 76923076923),
      ('HF21 not applied (NULL): rounded down',      1000000000001, TRUE,  TRUE,  NULL,  76923076923),
      ('post-HF16 pre-HF21, 5 VESTS: minimum of 1',  5,             TRUE,  TRUE,  FALSE, 1),
      ('pre-HF16: 104 intervals, rounded down',      1000000000001, TRUE,  FALSE, FALSE, 9615384615),
      ('pre-HF16, 50 VESTS: minimum of 1',           50,            TRUE,  FALSE, FALSE, 1),
      ('pre-HF1: precision multiplier',              1000,          FALSE, FALSE, FALSE, 9615384),
      ('cancel: 0',                                  0,             TRUE,  TRUE,  TRUE,  0)
    ) AS c(label, amount, hf01, hf16, hf21, expected)
  LOOP
    __actual := (btracker_backend.process_withdraw_vesting_operation(
      pg_temp.withdraw_vesting_body('alice', __case.amount), __case.hf01, __case.hf16, __case.hf21)).vesting_withdraw_rate;
    IF __actual IS DISTINCT FROM __case.expected THEN
      __failures := __failures || format('%s: amount %s, expected %s, found %s', __case.label, __case.amount, __case.expected, __actual);
    END IF;
  END LOOP;

  IF cardinality(__failures) > 0 THEN
    RAISE EXCEPTION 'process_withdraw_vesting_operation: %', array_to_string(__failures, '; ');
  END IF;
  RAISE NOTICE 'PASS: process_withdraw_vesting_operation applies hived''s rounding in every hardfork era';
END
$$;

BEGIN;
DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
  __block CONSTANT INT := hive.app_get_current_block_num(__schema);
  __amount CONSTANT BIGINT := 1000000000001;
  __intervals INT;
  __round_up BOOLEAN;
  __expected BIGINT;
  __a TEXT;
  __b TEXT;
  __actual JSONB;
BEGIN
  SELECT av.name INTO __a FROM hive.accounts_view av ORDER BY av.id OFFSET 10 LIMIT 1;
  SELECT av.name INTO __b FROM hive.accounts_view av ORDER BY av.id OFFSET 11 LIMIT 1;
  IF __b IS NULL THEN
    RAISE EXCEPTION 'no fixture accounts: % %', __a, __b;
  END IF;

  -- The test HAF's head decides the era the processing must apply.
  SELECT
    CASE WHEN bool_or(hardfork_num = btracker_backend.hf_withdraw_rate() AND block_num < __block) THEN 13 ELSE 104 END,
    COALESCE(bool_or(hardfork_num = btracker_backend.hf_withdraw_rate_round_up() AND block_num < __block), FALSE)
  INTO __intervals, __round_up
  FROM hafd.applied_hardforks;
  __expected := __amount / __intervals + CASE WHEN __round_up AND __amount % __intervals <> 0 THEN 1 ELSE 0 END;

  -- Operations past the block's own ones (op_pos from 10000), so their ids sort after them.
  DROP TABLE IF EXISTS _btracker_ops_batch;
  CREATE TEMP TABLE _btracker_ops_batch AS
  SELECT hafd.operation_id(__block, o.pos) AS id,
         __block AS block_num, -1::SMALLINT AS trx_in_block, o.pos AS op_pos,
         btracker_backend.op_withdraw_vesting()::SMALLINT AS op_type_id,
         pg_temp.withdraw_vesting_body(o.account, o.amount) AS body_value,
         NULL::INT AS custom_json_type_id
  FROM (VALUES (10001, __a, __amount), (10002, __b, 5::BIGINT)) AS o(pos, account, amount);
  PERFORM process_block_range_balances(__block, __block);
  PERFORM process_block_range_withdrawals(__block, __block);

  SELECT jsonb_object_agg(av.name, jsonb_build_array(aw.vesting_withdraw_rate, aw.to_withdraw, aw.withdrawn)) INTO __actual
  FROM account_withdraws aw JOIN hive.accounts_view av ON av.id = aw.account
  WHERE av.name IN (__a, __b);
  IF __actual IS DISTINCT FROM jsonb_build_object(
       __a, jsonb_build_array(__expected, __amount, 0),
       __b, jsonb_build_array(1, 5, 0)) THEN
    RAISE EXCEPTION 'process_block_range_withdrawals at block % (% intervals, round up %): expected % [%, %, 0] and % [1, 5, 0], found %',
      __block, __intervals, __round_up, __a, __expected, __amount, __b, __actual;
  END IF;
  RAISE NOTICE 'PASS: process_block_range_withdrawals stores the rate of block %''s era (% intervals, round up %) and the minimum of 1',
    __block, __intervals, __round_up;
END
$$;
ROLLBACK;

BEGIN;
DO
$$
DECLARE
  __name CONSTANT TEXT := 'vesting_withdraw_rate_hf21_rounding';
  __installed INT;
  __stale INT[];
  __diff BIGINT;
  __recorded INT;
BEGIN
  SELECT rows_repaired INTO __installed FROM one_off_repairs WHERE name = __name;
  IF __installed IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'expected the fresh install to record % with 0 rows, found %', __name, __installed;
  END IF;

  CREATE TEMP TABLE _withdraws_before ON COMMIT DROP AS SELECT * FROM account_withdraws;

  -- Active power-downs started by a withdraw_vesting, given the rate an earlier version
  -- would have stored for a post-HF21 amount that does not divide evenly.
  SELECT array_agg(account) INTO __stale FROM (
    SELECT aw.account
    FROM account_withdraws aw
    JOIN operations_view ov ON ov.id = aw.source_op
    WHERE aw.to_withdraw > 0 AND aw.vesting_withdraw_rate > 1
      AND ov.op_type_id = btracker_backend.op_withdraw_vesting()
    ORDER BY aw.account
    LIMIT 3) s;
  IF cardinality(__stale) IS DISTINCT FROM 3 THEN
    RAISE EXCEPTION 'expected 3 active power-downs in the synced data, found %', __stale;
  END IF;
  UPDATE account_withdraws SET vesting_withdraw_rate = vesting_withdraw_rate - 1 WHERE account = ANY(__stale);

  PERFORM run_one_off_repairs();
  IF (SELECT count(*) FROM account_withdraws aw JOIN _withdraws_before b USING (account)
      WHERE aw.vesting_withdraw_rate = b.vesting_withdraw_rate - 1) <> 3 THEN
    RAISE EXCEPTION 'run_one_off_repairs applied % again although it is recorded', __name;
  END IF;
  RAISE NOTICE 'PASS: run_one_off_repairs skips a recorded repair';

  DELETE FROM one_off_repairs WHERE name = __name;
  PERFORM run_one_off_repairs();

  SELECT count(*) INTO __diff FROM (
    (SELECT * FROM account_withdraws EXCEPT ALL SELECT * FROM _withdraws_before)
    UNION ALL
    (SELECT * FROM _withdraws_before EXCEPT ALL SELECT * FROM account_withdraws)) d;
  IF __diff <> 0 THEN
    RAISE EXCEPTION 'after the repair % account_withdraws rows differ from the synced state (stale accounts %)', __diff, __stale;
  END IF;
  SELECT rows_repaired INTO __recorded FROM one_off_repairs WHERE name = __name;
  IF __recorded IS DISTINCT FROM 3 THEN
    RAISE EXCEPTION 'expected % recorded with 3 rows, found %', __name, __recorded;
  END IF;
  RAISE NOTICE 'PASS: the repair restores the 3 stale rates, records itself and leaves every other row unchanged';

  IF repair_vesting_withdraw_rate() <> 0 THEN
    RAISE EXCEPTION 'repair_vesting_withdraw_rate changed rows of a repaired schema';
  END IF;
  RAISE NOTICE 'PASS: repair_vesting_withdraw_rate changes nothing on a repaired schema';
END
$$;
ROLLBACK;
