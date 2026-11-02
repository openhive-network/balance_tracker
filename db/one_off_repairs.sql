SET ROLE btracker_owner;

/**
 * One-off repairs of already synced state.
 * ========================================
 * A fix to block processing only corrects the rows it rewrites from then on;
 * rows an earlier version wrote stay wrong until an operation touches them
 * again. A repair recomputes those rows once per install: install_app.sh runs
 * run_one_off_repairs() on every install and upgrade, which applies each repair
 * not yet recorded in one_off_repairs and records it.
 *
 * On a fresh install the tables are empty, so each repair changes nothing and
 * is recorded as applied; the fixed processing writes every row from then on.
 *
 * one_off_repairs is not registered with the HAF context: a fork rollback must
 * not make a repair run again.
 *
 * To add a repair: write a function returning the number of rows it changed,
 * and add (name, function) to the list in run_one_off_repairs(). A repair that
 * edits a registered table must not issue the UPDATE or DELETE when it has
 * nothing to change: HAF's statement-level triggers reject any table edit on a forking
 * context that has not processed a block yet (a fresh install).
 */
CREATE TABLE IF NOT EXISTS one_off_repairs
(
  name          TEXT        NOT NULL,
  rows_repaired INT         NOT NULL,
  applied_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT pk_one_off_repairs PRIMARY KEY (name)
);

/**
 * repair_vesting_withdraw_rate()
 * ------------------------------
 * Recomputes account_withdraws.vesting_withdraw_rate of every active power-down
 * from the withdraw_vesting operation that started it (account_withdraws.source_op),
 * with the rounding process_withdraw_vesting_operation applies (minimum of 1,
 * rounded up from HF21 on). Earlier versions always rounded down.
 *
 * @returns number of power-downs whose rate changed
 */
CREATE OR REPLACE FUNCTION repair_vesting_withdraw_rate()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_withdraw_vesting              INT := btracker_backend.op_withdraw_vesting();
  _hf_vests_precision               INT := btracker_backend.hf_vests_precision();
  _hf_withdraw_rate                 INT := btracker_backend.hf_withdraw_rate();
  _hf_withdraw_rate_round_up        INT := btracker_backend.hf_withdraw_rate_round_up();
  _hf_vests_precision_block         INT;
  _hf_withdraw_rate_block           INT;
  _hf_withdraw_rate_round_up_block  INT;
  __accounts                        INT[];
  __rates                           BIGINT[];
  __repaired                        INT;
BEGIN
  SELECT
    MAX(CASE WHEN hardfork_num = _hf_vests_precision THEN block_num END),
    MAX(CASE WHEN hardfork_num = _hf_withdraw_rate THEN block_num END),
    MAX(CASE WHEN hardfork_num = _hf_withdraw_rate_round_up THEN block_num END)
  INTO _hf_vests_precision_block, _hf_withdraw_rate_block, _hf_withdraw_rate_round_up_block
  FROM hafd.applied_hardforks
  WHERE hardfork_num IN (_hf_vests_precision, _hf_withdraw_rate, _hf_withdraw_rate_round_up);

  SELECT array_agg(r.account), array_agg(r.vesting_withdraw_rate)
  INTO __accounts, __rates
  FROM (
    SELECT
      aw.account,
      aw.vesting_withdraw_rate AS stored_rate,
      (btracker_backend.process_withdraw_vesting_operation(
        ov.body_value,
        ov.block_num > _hf_vests_precision_block,
        ov.block_num > _hf_withdraw_rate_block,
        ov.block_num > _hf_withdraw_rate_round_up_block
      )).vesting_withdraw_rate
    FROM account_withdraws aw
    -- OFFSET 0 keeps the lookup per power-down (a primary-key probe each) instead of
    -- a hash join over every withdraw_vesting operation of the chain.
    CROSS JOIN LATERAL (
      SELECT o.body_value, o.block_num
      FROM operations_view o
      WHERE o.id = aw.source_op AND o.op_type_id = _op_withdraw_vesting
      OFFSET 0
    ) ov
    WHERE aw.to_withdraw > 0
  ) r
  WHERE r.vesting_withdraw_rate IS DISTINCT FROM r.stored_rate;

  IF __accounts IS NULL THEN
    RETURN 0;
  END IF;

  UPDATE account_withdraws aw
  SET vesting_withdraw_rate = u.rate
  FROM unnest(__accounts, __rates) AS u(account, rate)
  WHERE aw.account = u.account;

  GET DIAGNOSTICS __repaired = ROW_COUNT;
  RETURN __repaired;
END
$$;

/**
 * repair_rc_delegation_overflow()
 * -------------------------------
 * Applies hived's implicit RC delegation reduction (remove_rc_delegation_overflow,
 * db/process_rc_delegations.sql) to every delegator whose stored RC delegations
 * exceed its effective vests now:
 *   VESTS balance - delegated vests + received vests - next power-down withdrawal
 * Earlier versions never reduced them. A reduced delegation keeps its source_op.
 *
 * It corrects only the delegations still over their delegator's vesting: one hived
 * reduced at an earlier vest change, after which the delegator's vesting grew back,
 * stays as stored, since the vesting at that change is not kept. Those need a
 * resync to be corrected.
 *
 * Runs after repair_vesting_withdraw_rate, whose rate it reads.
 *
 * @returns number of RC delegations reduced or removed
 */
CREATE OR REPLACE FUNCTION repair_rc_delegation_overflow()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _nai_vests          INT := btracker_backend.nai_vests();
  __delegators        INT[];
  __effective_vests   BIGINT[];
BEGIN
  SELECT array_agg(d.delegator), array_agg(
    COALESCE(cab.balance, 0) - COALESCE(ad.delegated_vests, 0) + COALESCE(ad.received_vests, 0)
    - CASE WHEN aw.to_withdraw > aw.withdrawn THEN LEAST(aw.vesting_withdraw_rate, aw.to_withdraw - aw.withdrawn) ELSE 0 END)
  INTO __delegators, __effective_vests
  FROM (SELECT DISTINCT crd.delegator FROM current_rc_delegations crd) d
  LEFT JOIN current_account_balances cab ON cab.account = d.delegator AND cab.nai = _nai_vests
  LEFT JOIN account_delegations ad ON ad.account = d.delegator
  LEFT JOIN account_withdraws aw ON aw.account = d.delegator;

  IF __delegators IS NULL THEN
    RETURN 0;
  END IF;

  RETURN remove_rc_delegation_overflow(__delegators, __effective_vests, array_fill(NULL::BIGINT, ARRAY[cardinality(__delegators)]));
END
$$;

/**
 * repair_hf_23_withdraw_requests()
 * --------------------------------
 * Deletes the pending savings withdrawals hived removed at HF23 and earlier
 * versions of process_hf_23 left in transfer_saving_id (see
 * delete_hf_23_withdraw_requests). Does nothing until the application has
 * processed the HF23 block: process_hf_23 deletes them then.
 *
 * @returns number of transfer_saving_id rows deleted
 */
CREATE OR REPLACE FUNCTION repair_hf_23_withdraw_requests()
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __hardfork_23_block INT := (SELECT block_num FROM hafd.applied_hardforks WHERE hardfork_num = 23);
BEGIN
  IF __hardfork_23_block IS NULL
     OR COALESCE(hive.app_get_current_block_num(current_schema()), 0) < __hardfork_23_block THEN
    RETURN 0;
  END IF;

  RETURN delete_hf_23_withdraw_requests(__hardfork_23_block);
END
$$;

/**
 * delete_hf_23_withdraw_requests(__hardfork_23_block)
 * ---------------------------------------------------
 * Deletes the transfer_saving_id rows of the accounts cleared at HF23 whose
 * transfer_from_savings was requested up to the HF23 block: hived removed those
 * withdrawals without a fill or cancel. Rows of requests made after HF23 are kept.
 * Only valid once the application has processed the HF23 block.
 *
 * @param __hardfork_23_block  block of HF23
 * @returns number of transfer_saving_id rows deleted
 */
CREATE OR REPLACE FUNCTION delete_hf_23_withdraw_requests(__hardfork_23_block INT)
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  _op_transfer_from_savings  INT := btracker_backend.op_transfer_from_savings();
  __accounts                 INT[];
  __request_ids              BIGINT[];
  __repaired                 INT;
BEGIN
  SELECT array_agg(tsi.account), array_agg(tsi.request_id)
  INTO __accounts, __request_ids
  FROM btracker_backend.hf_23_cleared_accounts(__hardfork_23_block) c
  JOIN transfer_saving_id tsi ON tsi.account = c.account_id
  JOIN accounts_view av ON av.id = tsi.account
  -- The account's latest request with this id: hived frees a removed request's
  -- id, so a request made after HF23 may reuse it.
  CROSS JOIN LATERAL (
    SELECT ov.block_num
    -- OFFSET 0 keeps both lookups per account (its transfer_from_savings by
    -- account and type, then a primary-key probe each) instead of a scan of
    -- every operation of the chain for the body filters.
    FROM (
      SELECT aov.operation_id
      FROM account_operations_view aov
      WHERE aov.account_id = tsi.account AND aov.op_type_id = _op_transfer_from_savings
      OFFSET 0
    ) aov
    CROSS JOIN LATERAL (
      SELECT o.block_num, o.body_value
      FROM operations_view o
      WHERE o.id = aov.operation_id
      OFFSET 0
    ) ov
    WHERE ov.body_value->>'from' = av.name
      AND (ov.body_value->>'request_id')::BIGINT = tsi.request_id
    ORDER BY aov.operation_id DESC
    LIMIT 1
  ) req
  WHERE req.block_num <= __hardfork_23_block;

  IF __accounts IS NULL THEN
    RETURN 0;
  END IF;

  DELETE FROM transfer_saving_id tsi
  USING unnest(__accounts, __request_ids) AS s(account, request_id)
  WHERE tsi.account = s.account AND tsi.request_id = s.request_id;

  GET DIAGNOSTICS __repaired = ROW_COUNT;
  RETURN __repaired;
END
$$;

/**
 * run_one_off_repairs()
 * ---------------------
 * Applies, in list order, every repair not yet recorded in one_off_repairs, each
 * recorded in the same transaction as its changes.
 */
CREATE OR REPLACE FUNCTION run_one_off_repairs()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __repair    RECORD;
  __repaired  INT;
BEGIN
  FOR __repair IN
    SELECT r.name, r.function_name
    FROM (VALUES
      (1, 'vesting_withdraw_rate_hf21_rounding', 'repair_vesting_withdraw_rate'),
      (2, 'rc_delegation_overflow', 'repair_rc_delegation_overflow'),
      (3, 'hf_23_cleared_withdraw_requests', 'repair_hf_23_withdraw_requests')
    ) AS r(position, name, function_name)
    WHERE NOT EXISTS (SELECT 1 FROM one_off_repairs a WHERE a.name = r.name)
    ORDER BY r.position
  LOOP
    EXECUTE format('SELECT %I()', __repair.function_name) INTO __repaired;
    INSERT INTO one_off_repairs (name, rows_repaired) VALUES (__repair.name, __repaired);
    RAISE NOTICE 'One-off repair % applied: % rows changed', __repair.name, __repaired;
  END LOOP;
END
$$;

RESET ROLE;
