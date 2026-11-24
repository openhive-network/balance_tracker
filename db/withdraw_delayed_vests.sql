SET ROLE btracker_owner;

/**
 * Delayed VESTS taken by power-down payouts
 * =========================================
 * withdraw_delayed_vests holds, for each fill_vesting_withdraw that took VESTS from
 * its withdrawer's delayed VESTS (account_withdraws.delayed_vests, a power-up still
 * in its 30-day delay), the VESTS it took, written by process_block_range_withdrawals.
 * hived takes a payout (all of a withdrawer's fills in a block) from the delayed VESTS
 * first: a payout of W VESTS with D
 * delayed, S of them routed back to the withdrawer as VESTS, erases min(W - S, D) of
 * them and lowers the withdrawer's governance weight by max(0, W - S - D) only
 * (database_vesting.cpp process_vesting_withdrawals, delayed_voting::update_votes).
 * A fill without a row took nothing from them; the weight change of a fill is its
 * (negative) VESTS change plus what it took.
 *
 * Block processing computes what a fill takes from the stored delayed_vests on, so
 * the table is complete for every block processed by a version that writes it; an
 * upgraded install has no rows for earlier fills. scripts/install_app.sh runs this
 * file in one transaction with db/process_withdrawals.sql: with the application
 * running during an upgrade, the first batch that sees the table also runs the
 * version that writes it, and none processes fills without recording them. Once
 * the table exists a re-run changes nothing.
 *
 * Its index follows the app index lifecycle: registered while massive sync has
 * dropped the others, it is built at finalize; on a finalized install it is built
 * here.
 */
DO $$
DECLARE
  __schema_name VARCHAR;
  __index TEXT;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;

  IF to_regclass(format('%I.withdraw_delayed_vests', __schema_name)) IS NULL THEN
    CREATE TABLE withdraw_delayed_vests
    (
      account       INT    NOT NULL, -- The withdrawer's account id
      source_op     BIGINT NOT NULL, -- The fill_vesting_withdraw operation
      delayed_vests BIGINT NOT NULL  -- The VESTS it took from the withdrawer's delayed VESTS (> 0)
    );
    PERFORM hive.app_register_table( __schema_name, 'withdraw_delayed_vests', __schema_name );
  END IF;

  __index := format('CREATE UNIQUE INDEX IF NOT EXISTS idx_withdraw_delayed_vests_account_source_op ON %I.withdraw_delayed_vests(account, source_op)', __schema_name);
  IF isIndexesCreated() THEN
    EXECUTE __index;
  END IF;
  PERFORM hive.app_register_index_dependency(__schema_name, __index);
END
$$;

RESET ROLE;
