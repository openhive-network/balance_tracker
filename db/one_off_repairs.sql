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
 * updates a registered table must not issue the UPDATE when it has nothing to
 * change: HAF's statement-level triggers reject any table edit on a forking
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
      (1, 'vesting_withdraw_rate_hf21_rounding', 'repair_vesting_withdraw_rate')
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
