/*
 * hf_23_withdraw_requests.sql
 * ===========================
 * The pending savings withdrawals of the accounts cleared at HF23, which hived
 * removed without a fill or cancel operation:
 *
 *   - process_hf_23 deletes a cleared account's transfer_saving_id rows and zeroes
 *     its savings_withdraw_requests, leaving every other row unchanged;
 *   - the one-off repair (hf_23_cleared_withdraw_requests): the install recorded it
 *     with nothing to change, repair_hf_23_withdraw_requests does nothing before the
 *     application has processed HF23, and delete_hf_23_withdraw_requests deletes a
 *     cleared account's rows requested up to the HF23 block, keeps those requested
 *     after it (also one reusing a removed request's id) and every other row, and
 *     changes nothing when run again.
 *
 * The test HAF stops at block 5,000,000, before HF23 and with too few savings
 * withdrawals: session temp views named operations_view and account_operations_view,
 * which the application's unqualified references resolve to first, add to the
 * context's views a hardfork_hive operation clearing a real account in a pretend
 * HF23 block H (10 blocks below the head) and that account's transfer_from_savings
 * requests: 1 at block H, 2 at block H + 1, 3 at H - 5 and again at H + 2.
 *
 * Expects psql variable `schema` (the application schema, also its context name)
 * and a synced, finalized schema. The checks run in transactions that are rolled
 * back. Run under `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SELECT set_config('btracker_test.schema', :'schema', false);
SET search_path TO :"schema", public;

CREATE TEMP TABLE _hf23_fixture AS
SELECT av.id AS account, av.name, hive.app_get_current_block_num(:'schema') - 10 AS hf_block
FROM accounts_view av ORDER BY av.id OFFSET 12 LIMIT 1;

-- (block offset from H, op_pos, request_id) of the account's synthetic transfer_from_savings
CREATE TEMP TABLE _hf23_requests AS
SELECT * FROM (VALUES (0, 10001, 1::BIGINT), (1, 10001, 2), (-5, 10001, 3), (2, 10001, 3)) AS r(block_offset, op_pos, request_id);

CREATE TEMP TABLE _hf23_ops AS
SELECT hafd.operation_id(f.hf_block, 10000) AS id, f.hf_block AS block_num,
       btracker_backend.op_hardfork_hive()::SMALLINT AS op_type_id,
       jsonb_build_object('account', f.name, 'treasury', 'steem.dao') AS body_value,
       NULL::INT AS account_id
FROM _hf23_fixture f
UNION ALL
SELECT hafd.operation_id(f.hf_block + r.block_offset, r.op_pos), f.hf_block + r.block_offset,
       btracker_backend.op_transfer_from_savings()::SMALLINT,
       jsonb_build_object('from', f.name, 'to', f.name, 'request_id', r.request_id,
         'amount', jsonb_build_object('amount', '1000', 'precision', 3, 'nai', '@@000000021'), 'memo', ''),
       f.account
FROM _hf23_fixture f CROSS JOIN _hf23_requests r;

DO
$$
DECLARE
  __schema CONSTANT TEXT := current_setting('btracker_test.schema');
BEGIN
  IF (SELECT count(*) FROM _hf23_fixture) <> 1 THEN
    RAISE EXCEPTION 'no fixture account';
  END IF;

  EXECUTE format($sql$
    CREATE TEMP VIEW operations_view AS
    SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value FROM %1$I.operations_view ov
    UNION ALL
    SELECT o.id, o.block_num, o.op_type_id, o.body_value FROM _hf23_ops o
  $sql$, __schema);

  EXECUTE format($sql$
    CREATE TEMP VIEW account_operations_view AS
    SELECT aov.account_id, aov.operation_id, aov.op_type_id, aov.block_num FROM %1$I.account_operations_view aov
    UNION ALL
    SELECT o.account_id, o.id, o.op_type_id, o.block_num FROM _hf23_ops o WHERE o.account_id IS NOT NULL
  $sql$, __schema);
END
$$;

CREATE FUNCTION pg_temp.stage_withdraw_requests()
RETURNS VOID LANGUAGE plpgsql
AS $f$
BEGIN
  DELETE FROM transfer_saving_id WHERE account = (SELECT account FROM _hf23_fixture);
  INSERT INTO transfer_saving_id (account, nai, balance, request_id)
  SELECT DISTINCT f.account, 21::SMALLINT, -1000::BIGINT, r.request_id FROM _hf23_fixture f CROSS JOIN _hf23_requests r;
  CREATE TEMP TABLE _transfer_saving_id_before ON COMMIT DROP AS SELECT * FROM transfer_saving_id;
END
$f$;

-- transfer_saving_id rows removed or added since stage_withdraw_requests(), as text.
CREATE FUNCTION pg_temp.transfer_saving_id_changes()
RETURNS TEXT LANGUAGE plpgsql
AS $f$
BEGIN
  RETURN (SELECT string_agg(format('%s (%s, %s)', c.change, c.account, c.request_id), ', ' ORDER BY c.change, c.account, c.request_id)
  FROM (
    (SELECT 'removed' AS change, t.account, t.request_id FROM (SELECT * FROM _transfer_saving_id_before EXCEPT ALL SELECT * FROM transfer_saving_id) t)
    UNION ALL
    (SELECT 'added', t.account, t.request_id FROM (SELECT * FROM transfer_saving_id EXCEPT ALL SELECT * FROM _transfer_saving_id_before) t)
  ) c);
END
$f$;

BEGIN;
DO
$$
DECLARE
  __f RECORD;
  __changes TEXT;
  __requests BIGINT;
BEGIN
  SELECT * INTO __f FROM _hf23_fixture;
  PERFORM pg_temp.stage_withdraw_requests();
  UPDATE account_savings SET savings_withdraw_requests = 3 WHERE account = __f.account;

  PERFORM btracker_backend.process_hf_23(__f.hf_block);

  __changes := pg_temp.transfer_saving_id_changes();
  IF __changes IS DISTINCT FROM format('removed (%1$s, 1), removed (%1$s, 2), removed (%1$s, 3)', __f.account) THEN
    RAISE EXCEPTION 'process_hf_23: expected exactly the cleared account %''s 3 withdrawal requests removed, found %', __f.account, __changes;
  END IF;

  SELECT COALESCE(sum(savings_withdraw_requests), 0) INTO __requests FROM account_savings WHERE account = __f.account;
  IF __requests <> 0 THEN
    RAISE EXCEPTION 'process_hf_23: account % still has % savings_withdraw_requests', __f.account, __requests;
  END IF;
  RAISE NOTICE 'PASS: process_hf_23 deletes the cleared account''s pending withdrawals and leaves every other row unchanged';
END
$$;
ROLLBACK;

BEGIN;
DO
$$
DECLARE
  __name CONSTANT TEXT := 'hf_23_cleared_withdraw_requests';
  __f RECORD;
  __installed INT;
  __deleted INT;
  __changes TEXT;
BEGIN
  SELECT * INTO __f FROM _hf23_fixture;

  SELECT rows_repaired INTO __installed FROM one_off_repairs WHERE name = __name;
  IF __installed IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'expected the install to record % with 0 rows, found %', __name, __installed;
  END IF;

  PERFORM pg_temp.stage_withdraw_requests();

  __deleted := repair_hf_23_withdraw_requests();
  __changes := pg_temp.transfer_saving_id_changes();
  IF __deleted <> 0 OR __changes IS NOT NULL THEN
    RAISE EXCEPTION 'repair_hf_23_withdraw_requests before the application processed HF23: deleted % rows, changes %', __deleted, __changes;
  END IF;
  RAISE NOTICE 'PASS: repair_hf_23_withdraw_requests changes nothing before HF23 is processed';

  __deleted := delete_hf_23_withdraw_requests(__f.hf_block);
  __changes := pg_temp.transfer_saving_id_changes();
  IF __deleted <> 1 OR __changes IS DISTINCT FROM format('removed (%s, 1)', __f.account) THEN
    RAISE EXCEPTION 'delete_hf_23_withdraw_requests at block %: expected only (%, 1) removed, deleted % rows, changes %',
      __f.hf_block, __f.account, __deleted, __changes;
  END IF;
  RAISE NOTICE 'PASS: the repair deletes the request made up to HF23 and keeps those made after it (one reusing an id) and every other row';

  IF delete_hf_23_withdraw_requests(__f.hf_block) <> 0 THEN
    RAISE EXCEPTION 'delete_hf_23_withdraw_requests changed rows of a repaired schema';
  END IF;
  RAISE NOTICE 'PASS: delete_hf_23_withdraw_requests changes nothing on a repaired schema';
END
$$;
ROLLBACK;
