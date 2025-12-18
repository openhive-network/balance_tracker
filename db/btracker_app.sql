/**
 * Balance Tracker Core Application Schema
 * =======================================
 *
 * This file defines the core Balance Tracker HAF application, including:
 * - HAF context registration and synchronization stages
 * - All database tables for tracking balances and related data
 * - Control functions for application lifecycle
 * - Block processing dispatch functions
 * - Index creation for API performance
 *
 * Table Groups:
 * -------------
 * 1. CONTROL TABLES
 *    - btracker_app_status: Processing control flag
 *    - version: Schema version tracking
 *    - asset_table: Supported assets (HIVE, HBD, VESTS)
 *
 * 2. ACCOUNT BALANCES
 *    - current_account_balances: Current liquid balances (latest state)
 *    - account_balance_history: Complete history of all balance changes
 *    - balance_history_by_day/month: Aggregated balance snapshots
 *
 * 3. REWARDS
 *    - account_rewards: Pending unclaimed rewards
 *    - account_info_rewards: Lifetime posting/curation rewards
 *
 * 4. DELEGATIONS
 *    - current_accounts_delegations: Active delegation pairs
 *    - account_delegations: Summary per account (total received/delegated)
 *
 * 5. RECURRENT TRANSFERS
 *    - recurrent_transfers: Active scheduled recurring transfers
 *
 * 6. WITHDRAWALS (Power-down)
 *    - account_withdraws: Power-down state per account
 *    - account_routes: Vesting withdrawal routing rules
 *
 * 7. SAVINGS
 *    - account_savings: Current savings balances
 *    - account_savings_history: Complete savings history
 *    - transfer_saving_id: Pending savings withdrawal requests
 *    - saving_history_by_day/month: Aggregated savings snapshots
 *
 * 8. TRANSFER STATISTICS
 *    - transfer_stats_by_hour/day/month: Aggregated transfer volumes
 *
 * 9. MARKET & CONVERSIONS
 *    - convert_state: Pending HBD↔HIVE conversions
 *    - order_state: Open market limit orders
 *
 * 10. ESCROWS
 *    - escrow_state: Active escrow agreements
 *    - escrow_fees: Pending escrow agent fees
 *
 * HAF Synchronization Stages:
 * ---------------------------
 * - MASSIVE_PROCESSING: Initial sync, processes blocks in batches of 10000
 * - LIVE: Real-time sync, processes blocks one at a time
 *
 * Processing Flow:
 * ----------------
 * 1. main() starts the application loop
 * 2. btracker_process_blocks() dispatches based on current stage
 * 3. btracker_massive_processing() or btracker_single_processing() calls
 *    individual process_* functions for each data type
 *
 * @see builtin_roles.sql for database role definitions
 * @see process_*.sql files for individual processing logic
 */

SET ROLE btracker_owner;

DO $$
  DECLARE __schema_name VARCHAR;
  DECLARE __context_table VARCHAR:='hive.';
  synchronization_stages hive.application_stages;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  __context_table:=__context_table || __schema_name;

  synchronization_stages := ARRAY[hive.stage( 'MASSIVE_PROCESSING', 101, 10000, '20 seconds' ), hive.live_stage()]::hive.application_stages;

  RAISE NOTICE 'balance_tracker will be installed in schema % with context %', __schema_name, __schema_name;

  IF hive.app_context_exists(__schema_name) THEN
      RAISE NOTICE 'Context % already exists, it means all tables are already created and data installing is skipped', __schema_name;
      RETURN;
  END IF;

  PERFORM hive.app_create_context(
     _name =>__schema_name,
     _schema => __schema_name,
     _is_forking => current_setting('custom.is_forking')::BOOLEAN,
     _stages => synchronization_stages
  );

  RAISE NOTICE 'Attempting to create an application schema tables...';
  -- Indicates whether the application should continue processing blocks
  -- stopped using stopProcessing() function
  CREATE TABLE IF NOT EXISTS btracker_app_status (continue_processing BOOLEAN NOT NULL);
  INSERT INTO btracker_app_status (continue_processing) VALUES (True);

  -- version table
  CREATE TABLE IF NOT EXISTS version(git_hash TEXT);
  INSERT INTO version VALUES('unspecified (generate and apply set_version_in_sql.pgsql)');

  -- table of supported assets (HIVE, HBD, VESTS)
  CREATE TABLE IF NOT EXISTS asset_table
  (
    asset_symbol_nai SMALLINT NOT NULL, -- Type of asset symbol used in the operation
    asset_precision  INT      NOT NULL, -- Precision of assets
    nai_string       TEXT     NOT NULL, -- NAI string representation
    asset_name       TEXT     NOT NULL, -- readable asset name (e.g., HIVE, HBD)

    CONSTRAINT pk_asset_table PRIMARY KEY (asset_symbol_nai),
    CONSTRAINT uq_asset_table_nai_string UNIQUE (nai_string),
    CONSTRAINT uq_asset_table_asset_name UNIQUE (asset_name)
  );
  --- Prepopulate asset table with HIVE, HBD and VESTS
  INSERT INTO asset_table(asset_symbol_nai, asset_precision, nai_string, asset_name)
  VALUES (13,3,'@@000000013','HBD'),(21,3,'@@000000021','HIVE'),(37,6,'@@000000037','VESTS');

  ------------- CURRENT ACCOUNT BALANCES ----------------
  CREATE TABLE IF NOT EXISTS current_account_balances
  (
    account              INT      NOT NULL, -- Balance owner account id
    nai                  SMALLINT NOT NULL, -- Balance type (currency)
    balance              BIGINT   NOT NULL, -- Balance value (amount of held tokens)
    balance_change_count INT      NOT NULL, -- Number of balance changes (for pagination)
    source_op            BIGINT   NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_current_account_balances PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table( __schema_name, 'current_account_balances', __schema_name );

  ------------- ACCOUNT BALANCE HISTORY ----------------
  CREATE TABLE IF NOT EXISTS account_balance_history
  (
    account        INT      NOT NULL, -- Balance owner account id
    nai            SMALLINT NOT NULL, -- Balance type (currency)
    balance        BIGINT   NOT NULL, -- Balance value after a change
    balance_seq_no INT      NOT NULL, -- Sequence number of the balance change
    source_op      BIGINT   NOT NULL  -- The operation triggered given balance change

    /** source_op_block removed to reduce table size,
        can be extracted using hafd.operation_id_to_block_num.
    */
    --source_op_block INT NOT NULL, -- Block containing the source operation

    /** Because of bugs in blockchain at very begin, it was possible to make a transfer to self. See summon transfer in block 118570,
        like also `register` transfer in block 818601
        That's why constraint has been eliminated.
    */
    --CONSTRAINT pk_account_balance_history PRIMARY KEY (account, source_op_block, nai, source_op)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_balance_history', __schema_name );

  CREATE TABLE IF NOT EXISTS balance_history_by_month
  (
    account     INT       NOT NULL, -- Balance owner account id
    nai         SMALLINT  NOT NULL, -- Balance type (currency)
    updated_at  TIMESTAMP NOT NULL, -- Period end time

    balance     BIGINT    NOT NULL, -- Balance value after a period
    min_balance BIGINT    NOT NULL, -- Minimum balance during the period
    max_balance BIGINT    NOT NULL, -- Maximum balance during the period

    source_op   BIGINT    NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_balance_history_by_month PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'balance_history_by_month', __schema_name );

  CREATE TABLE IF NOT EXISTS balance_history_by_day
  (
    account     INT       NOT NULL, -- Balance owner account id
    nai         SMALLINT  NOT NULL, -- Balance type (currency)
    updated_at  TIMESTAMP NOT NULL, -- Period end time

    balance     BIGINT    NOT NULL, -- Balance value after a period
    min_balance BIGINT    NOT NULL, -- Minimum balance during the period
    max_balance BIGINT    NOT NULL, -- Maximum balance during the period

    source_op   BIGINT    NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_balance_history_by_day PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'balance_history_by_day', __schema_name );

  ------------- CURRENT ACCOUNT REWARDS ----------------
  CREATE TABLE IF NOT EXISTS account_rewards
  (
    account   INT      NOT NULL, -- Balance owner account id
    nai       SMALLINT NOT NULL, -- Balance type (currency)
    balance   BIGINT   NOT NULL, -- Balance value (amount of held tokens)
    source_op BIGINT   NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_account_rewards PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_rewards', __schema_name );

  -- summarized rewards info
  CREATE TABLE IF NOT EXISTS account_info_rewards
  (
    account          INT    NOT NULL,  -- Balance owner account id
    posting_rewards  BIGINT DEFAULT 0, -- Total posting rewards
    curation_rewards BIGINT DEFAULT 0, -- Total curation rewards

    CONSTRAINT pk_account_info_rewards PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_info_rewards', __schema_name );

  ------------- CURRENT ACCOUNT DELEGATIONS ----------------
  CREATE TABLE IF NOT EXISTS current_accounts_delegations
  (
    delegator INT    NOT NULL, -- ID of the delegator account
    delegatee INT    NOT NULL, -- ID of the delegatee account
    balance   BIGINT NOT NULL, -- Amount of delegated vests
    source_op BIGINT NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_current_accounts_delegations PRIMARY KEY (delegator, delegatee)
  );
  PERFORM hive.app_register_table( __schema_name, 'current_accounts_delegations', __schema_name );

  -- summarized delegated vests info
  CREATE TABLE IF NOT EXISTS account_delegations
  (
    account         INT    NOT NULL,  -- Balance owner account id
    received_vests  BIGINT DEFAULT 0, -- Total received vests
    delegated_vests BIGINT DEFAULT 0, -- Total delegated vests

    CONSTRAINT pk_temp_vests PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_delegations', __schema_name );

  ------------- RECURRENT TRANSFERS ----------------
  CREATE TABLE IF NOT EXISTS recurrent_transfers
  (
    from_account         INT      NOT NULL, -- ID of the account transfering
    to_account           INT      NOT NULL, -- ID of the account receiving

    /** In hf28 there is added functionality of pair_id for recurrent transfers,
        extracted from extensions object using extract_pair_id function.

        It allows to have multiple recurrent transfers between same accounts.
    */
    transfer_id          INT      NOT NULL, -- pair_id of the recurrent transfer
    nai                  SMALLINT NOT NULL, -- Type of asset symbol used in the operation
    amount               BIGINT   NOT NULL, -- Amount of asset to be transfered
    consecutive_failures INT      NOT NULL, -- Number of consecutive failures (fail_recurrent_transfer_operation)
    remaining_executions INT      NOT NULL, -- Number of remaining executions (after each execution decreased by 1)
    recurrence           INT      NOT NULL, -- Recurrence in number of hours
    memo                 TEXT     NOT NULL, -- Memo attached to the recurrent transfer
    source_op            BIGINT   NOT NULL, -- The operation triggered last change

    CONSTRAINT pk_recurrent_transfers PRIMARY KEY (from_account, to_account, transfer_id)
  );
  PERFORM hive.app_register_table( __schema_name, 'recurrent_transfers', __schema_name );

  ------------- ACCOUNT WITHDRAWS ----------------
  CREATE TABLE IF NOT EXISTS account_withdraws
  (
    account               INT    NOT NULL,  -- Account ID
    vesting_withdraw_rate BIGINT DEFAULT 0, -- Amount of VESTS to withdraw per interval
    to_withdraw           BIGINT DEFAULT 0, -- Amount of VESTS left to withdraw
    withdrawn             BIGINT DEFAULT 0, -- Amount of VESTS already withdrawn
    withdraw_routes       BIGINT DEFAULT 0, -- Number of withdraw routes (routes are accounts to which vesting is withdrawn)
    delayed_vests         BIGINT DEFAULT 0, -- Amount of VESTS in delayed withdraw ()
    source_op             BIGINT,           -- The operation triggered last change

    CONSTRAINT pk_account_withdraws PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_withdraws', __schema_name );

  CREATE TABLE IF NOT EXISTS account_routes
  (
    account    INT    NOT NULL, -- Account ID
    to_account INT    NOT NULL, -- Route account ID
    percent    INT    NOT NULL, -- Percentage of the withdraw routed to the to_account
    source_op  BIGINT NOT NULL, -- The operation triggered last change

    CONSTRAINT pk_account_routes PRIMARY KEY (account, to_account)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_routes', __schema_name );

  ------------- CURRENT ACCOUNT SAVING BALANCE ----------------
  CREATE TABLE IF NOT EXISTS account_savings
  (
    account                   INT      NOT NULL,  -- Balance owner account id
    nai                       SMALLINT NOT NULL,  -- Balance type (currency)
    balance                   BIGINT   DEFAULT 0, -- Balance value (amount of held tokens)
    balance_change_count      INT      NOT NULL,  -- Number of balance changes (for pagination)
    source_op                 BIGINT   NOT NULL,  -- The operation triggered last balance change
    savings_withdraw_requests INT      DEFAULT 0, -- Number of active withdraw requests (see transfer_saving_id table)

    CONSTRAINT pk_account_savings PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table( __schema_name, 'account_savings', __schema_name );

  -- Tracks transfer saving requests
  CREATE TABLE IF NOT EXISTS transfer_saving_id
  (
    account     INT      NOT NULL, -- ID of an account transfering from savings
    nai         SMALLINT NOT NULL, -- NAI of the saving balance
    balance     BIGINT   NOT NULL, -- Amount being transfered from savings
    request_id  BIGINT   NOT NULL, -- Unique per account request id

    CONSTRAINT pk_transfer_saving_id PRIMARY KEY (account, request_id)
  );
  PERFORM hive.app_register_table( __schema_name, 'transfer_saving_id', __schema_name );

  ------------- ACCOUNT SAVING BALANCE HISTORY ----------------
  CREATE TABLE IF NOT EXISTS account_savings_history
  (
    account        INT      NOT NULL, -- Balance owner account id
    nai            SMALLINT NOT NULL, -- Balance type (currency)
    balance        BIGINT   NOT NULL, -- Balance value after a change
    balance_seq_no INT      NOT NULL, -- Sequence number of the balance change
    source_op      BIGINT   NOT NULL  -- The operation triggered given balance change
  );
  PERFORM hive.app_register_table( __schema_name, 'account_savings_history', __schema_name );

  CREATE TABLE IF NOT EXISTS saving_history_by_month
  (
    account     INT       NOT NULL, -- Balance owner account id
    nai         SMALLINT  NOT NULL, -- Balance type (currency)
    updated_at  TIMESTAMP NOT NULL, -- Period end time

    balance     BIGINT    NOT NULL, -- Balance value after a period
    min_balance BIGINT    NOT NULL, -- Minimum balance during the period
    max_balance BIGINT    NOT NULL, -- Maximum balance during the period

    source_op   BIGINT    NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_saving_history_by_month PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'saving_history_by_month', __schema_name );

  CREATE TABLE IF NOT EXISTS saving_history_by_day
  (
    account     INT       NOT NULL, -- Balance owner account id
    nai         SMALLINT  NOT NULL, -- Balance type (currency)
    updated_at  TIMESTAMP NOT NULL, -- Period end time

    balance     BIGINT    NOT NULL, -- Balance value after a period
    min_balance BIGINT    NOT NULL, -- Minimum balance during the period
    max_balance BIGINT    NOT NULL, -- Maximum balance during the period

    source_op   BIGINT    NOT NULL, -- The operation triggered last balance change

    CONSTRAINT pk_saving_history_by_day PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'saving_history_by_day', __schema_name );

  ------------- TRANSFER STATISTICS ----------------
  CREATE TABLE IF NOT EXISTS transfer_stats_by_month
  (
    nai                 SMALLINT  NOT NULL, -- NAI of the transfer
    updated_at          TIMESTAMP NOT NULL, -- Period end time
    sum_transfer_amount BIGINT    NOT NULL, -- Total amount of transfered tokens
    max_transfer_amount BIGINT    NOT NULL, -- Maximum single transfer amount
    min_transfer_amount BIGINT    NOT NULL, -- Minimum single transfer amount
    transfer_count      INT       NOT NULL, -- Number of transfers during the period
    last_block_num      INT       NOT NULL, -- Last block number included in the statistics

    CONSTRAINT pk_transfer_stats_by_month PRIMARY KEY (nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'transfer_stats_by_month', __schema_name );

  CREATE TABLE IF NOT EXISTS transfer_stats_by_day
  (
    nai                 SMALLINT  NOT NULL, -- NAI of the transfer
    updated_at          TIMESTAMP NOT NULL, -- Period end time
    sum_transfer_amount BIGINT    NOT NULL, -- Total amount of transfers
    max_transfer_amount BIGINT    NOT NULL, -- Maximum single transfer amount
    min_transfer_amount BIGINT    NOT NULL, -- Minimum single transfer amount
    transfer_count      INT       NOT NULL, -- Number of transfers during the period
    last_block_num      INT       NOT NULL, -- Last block number included in the statistics

    CONSTRAINT pk_transfer_stats_by_day PRIMARY KEY (nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'transfer_stats_by_day', __schema_name );

  CREATE TABLE IF NOT EXISTS transfer_stats_by_hour
  (
    nai                 SMALLINT  NOT NULL, -- NAI of the transfer
    updated_at          TIMESTAMP NOT NULL, -- Period end time
    sum_transfer_amount BIGINT    NOT NULL, -- Total amount of transfers
    max_transfer_amount BIGINT    NOT NULL, -- Maximum single transfer amount
    min_transfer_amount BIGINT    NOT NULL, -- Minimum single transfer amount
    transfer_count      INT       NOT NULL, -- Number of transfers during the period
    last_block_num      INT       NOT NULL, -- Last block number included in the statistics

    CONSTRAINT pk_transfer_stats_by_hour PRIMARY KEY (nai, updated_at)
  );
  PERFORM hive.app_register_table( __schema_name, 'transfer_stats_by_hour', __schema_name );

  ------------- CONVERTS STATE ----------------
  CREATE TABLE IF NOT EXISTS convert_state
  (
    owner_id      INT      NOT NULL, -- hive.accounts_view.id
    request_id    BIGINT   NOT NULL, -- unique per owner
    nai           SMALLINT NOT NULL, -- 13=HBD, 21=HIVE
    remaining     BIGINT   NOT NULL, -- satoshis (HIVE/HBD ×1000)
    request_block INT      NOT NULL, -- block when convert was requested

    CONSTRAINT pk_convert_state PRIMARY KEY (owner_id, request_id, nai)
  );
  PERFORM hive.app_register_table(__schema_name, 'convert_state', __schema_name);

  ------------- ORDER STATE ----------------
  CREATE TABLE IF NOT EXISTS order_state
  (
    owner_id      INT      NOT NULL, -- hive.accounts_view.id
    order_id      BIGINT   NOT NULL, -- per-owner unique
    nai           SMALLINT NOT NULL, -- 13=HBD, 21=HIVE
    remaining     BIGINT   NOT NULL, -- satoshis (HIVE/HBD ×1000)
    block_created INT      NOT NULL, -- block when order was created

    CONSTRAINT pk_order_state PRIMARY KEY (owner_id, order_id)
  );
  PERFORM hive.app_register_table(__schema_name, 'order_state', __schema_name);

  ------------- ESCROW STATE ----------------
  CREATE TABLE IF NOT EXISTS escrow_state
  (
    from_id        INT      NOT NULL, -- hive.accounts_view.id
    escrow_id      BIGINT   NOT NULL, -- unique per "from"
    hive_nai       SMALLINT NOT NULL, -- 13=HBD, 21=HIVE
    hive_amount    BIGINT   NOT NULL, -- satoshis (×1000)
    hbd_nai        SMALLINT NOT NULL, -- 13=HBD, 21=HIVE
    hbd_amount     BIGINT   NOT NULL, -- satoshis (×1000)
    source_op      BIGINT   NOT NULL, -- last operation affecting escrow
    to_approved    BOOLEAN  NOT NULL DEFAULT FALSE, -- true if escrow_approved_operation was processed
    disputed       BOOLEAN  NOT NULL DEFAULT FALSE, -- true if escrow_dispute_operation was processed

    CONSTRAINT pk_escrow_state PRIMARY KEY (from_id, escrow_id)
  );
  PERFORM hive.app_register_table(__schema_name, 'escrow_state', __schema_name);

  CREATE TABLE IF NOT EXISTS escrow_fees
  (
    from_id        INT      NOT NULL, -- hive.accounts_view.id
    escrow_id      BIGINT   NOT NULL, -- unique per "from"
    nai            SMALLINT NOT NULL, -- 13=HBD, 21=HIVE
    fee_amount     BIGINT   NOT NULL, -- satoshis (×1000)

    CONSTRAINT pk_escrow_fee PRIMARY KEY (from_id, escrow_id)
  );
  PERFORM hive.app_register_table(__schema_name, 'escrow_fees', __schema_name);


END
$$;

-- ============================================================================
-- CONTROL FUNCTIONS
-- ============================================================================
-- These functions manage the application processing lifecycle.
-- Used by scripts/process_blocks.sh to control block processing.

/**
 * continueProcessing()
 * --------------------
 * Check if the application should continue processing blocks.
 * Called in the main loop to allow graceful shutdown.
 *
 * @returns TRUE if processing should continue, FALSE to stop
 */
CREATE OR REPLACE FUNCTION continueProcessing()
RETURNS BOOLEAN
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN continue_processing FROM btracker_app_status LIMIT 1;
END
$$;

/**
 * allowProcessing()
 * -----------------
 * Enable block processing. Called at application startup
 * to reset the processing flag after a previous stop.
 */
CREATE OR REPLACE FUNCTION allowProcessing()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE btracker_app_status SET continue_processing = True;
END
$$;

/**
 * stopProcessing()
 * ----------------
 * Signal the application to stop processing after the current block.
 * Must be called from a separate session and committed to take effect.
 * The main loop will exit gracefully on next iteration check.
 *
 * Usage: Call from psql or separate connection, then COMMIT.
 */
CREATE OR REPLACE FUNCTION stopProcessing()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE btracker_app_status SET continue_processing = False;
END
$$;

/**
 * isIndexesCreated()
 * ------------------
 * Check if performance indexes have been created.
 * Used to determine if we're transitioning from MASSIVE to LIVE stage.
 * Indexes are created once before entering LIVE processing.
 *
 * @returns TRUE if indexes exist, FALSE otherwise
 */
CREATE OR REPLACE FUNCTION isIndexesCreated()
RETURNS BOOLEAN
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN EXISTS(
      SELECT true FROM pg_index WHERE indexrelid = (
        SELECT oid FROM pg_class WHERE relname = 'idx_account_balance_nai_balance_idx' LIMIT 1
      )
    );
END
$$;

-- ============================================================================
-- BLOCK PROCESSING FUNCTIONS
-- ============================================================================
-- These functions handle block processing dispatch and execution.
-- Called by the main loop for each block range to sync.

/**
 * btracker_process_blocks()
 * -------------------------
 * Main dispatch function for block processing.
 * Routes to either massive or single processing based on current HAF stage.
 *
 * Behavior by stage:
 * - MASSIVE_PROCESSING: Calls btracker_massive_processing() for batch sync
 * - LIVE: Creates indexes (once), then calls btracker_single_processing()
 *
 * @param _context_name  HAF context name (typically schema name)
 * @param _block_range   Range of blocks to process (first_block, last_block)
 * @param _logs          Enable progress logging (default: true)
 */
CREATE OR REPLACE FUNCTION btracker_process_blocks(
    _context_name hive.context_name,
    _block_range hive.blocks_range,
    _logs BOOLEAN = true
)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$

BEGIN
  IF hive.get_current_stage_name(_context_name) = 'MASSIVE_PROCESSING' THEN
    CALL btracker_massive_processing(_block_range.first_block, _block_range.last_block, _logs);
    --PERFORM hive.app_request_table_vacuum('hafbe_bal.account_balance_history', interval '10 minutes'); --eventually fixup hard-coded schema name!
    RETURN;
  END IF;

  IF NOT isIndexesCreated() THEN
    PERFORM create_btracker_indexes();
  END IF;
  CALL btracker_single_processing(_block_range.first_block, _logs);
END
$$;

/**
 * btracker_massive_processing()
 * -----------------------------
 * Process a range of blocks during initial sync (MASSIVE_PROCESSING stage).
 * Optimized for throughput with synchronous_commit OFF.
 *
 * Special handling for HF23 (block 41818752):
 * The hardfork_hive_operation at HF23 lacks balance/rewards/savings info
 * for affected accounts. We split processing around HF23 and manually
 * process affected accounts via process_hf_23().
 *
 * Processing order per range:
 * 1. Balances (liquid)
 * 2. Withdrawals (power-down)
 * 3. Savings
 * 4. Rewards
 * 5. Delegations
 * 6. Recurrent transfers
 * 7. Transfer statistics
 * 8. Conversions
 * 9. Market orders
 * 10. Escrows
 *
 * @param _from  First block number to process
 * @param _to    Last block number to process
 * @param _logs  Enable progress logging
 */
CREATE OR REPLACE PROCEDURE btracker_massive_processing(
    IN _from INT,
    IN _to INT,
    IN _logs BOOLEAN
)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __start_ts timestamptz;
  __end_ts   timestamptz;
  __hardfork_23_block INT := (SELECT block_num FROM hafd.applied_hardforks WHERE hardfork_num = 23);
BEGIN
  PERFORM set_config('synchronous_commit', 'OFF', false);

  IF _logs THEN
    RAISE NOTICE 'Btracker is attempting to process a block range: <%, %>', _from, _to;
    __start_ts := clock_timestamp();
  END IF;

  -- TODO: remove this hack after the hardfork_hive_operation is fixed

  -- harfork_hive_operation lacks informations about rewards, savings and balances
  -- to use rewards, savings and balances processed in query rather by insterting every operation to table
  -- hf_23 accounts needs to be processed manually

  -- Check if the block number hf_23 is within the range
  IF __hardfork_23_block BETWEEN _from AND _to THEN
    -- Enter a loop iterating from _from to hf_23
    PERFORM process_block_range_balances(_from, __hardfork_23_block);
    PERFORM process_block_range_withdrawals(_from, __hardfork_23_block);
    PERFORM process_block_range_savings(_from, __hardfork_23_block);
    PERFORM process_block_range_rewards(_from, __hardfork_23_block);
    PERFORM process_block_range_delegations(_from, __hardfork_23_block);
    PERFORM process_block_range_recurrent_transfers(_from, __hardfork_23_block);
    PERFORM process_transfer_stats(_from, __hardfork_23_block);
    PERFORM process_block_range_converts(_from,        __hardfork_23_block);
    PERFORM process_block_range_orders(_from,        __hardfork_23_block);
    PERFORM process_block_range_escrows(_from,        __hardfork_23_block);

    -- Manually process hardfork_hive_operation for balance, rewards, savings
    PERFORM btracker_backend.process_hf_23(__hardfork_23_block);
    RAISE NOTICE 'Btracker processed hardfork 23 successfully';

    IF __hardfork_23_block != _to THEN
      -- Continue the loop from hf_23 to _to
      PERFORM process_block_range_balances(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_withdrawals(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_savings(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_rewards(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_delegations(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_recurrent_transfers(__hardfork_23_block + 1, _to);
      PERFORM process_transfer_stats(__hardfork_23_block + 1, _to);
      PERFORM process_block_range_converts(__hardfork_23_block+1, _to);
      PERFORM process_block_range_orders(__hardfork_23_block+1, _to);
      PERFORM process_block_range_escrows(__hardfork_23_block+1, _to);
    END IF;

  ELSE
    -- If 41818752 is not in range, process the full range normally
    PERFORM process_block_range_balances(_from, _to);
    PERFORM process_block_range_withdrawals(_from, _to);
    PERFORM process_block_range_savings(_from, _to);
    PERFORM process_block_range_rewards(_from, _to);
    PERFORM process_block_range_delegations(_from, _to);
    PERFORM process_block_range_recurrent_transfers(_from, _to);
    PERFORM process_transfer_stats(_from, _to);
    PERFORM process_block_range_converts(_from, _to);
    PERFORM process_block_range_orders(_from, _to);
    PERFORM process_block_range_escrows(_from, _to);
  END IF;

  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'Btracker processed block range: <%, %> successfully in % s
    ', _from, _to, (extract(epoch FROM __end_ts - __start_ts));
  END IF;
END
$$;

/**
 * btracker_single_processing()
 * ----------------------------
 * Process a single block during LIVE sync stage.
 * Uses synchronous_commit ON for data safety.
 *
 * Called for each new block after initial sync is complete.
 * Processes all operation types for the given block.
 *
 * @param _block  Block number to process
 * @param _logs   Enable progress logging
 */
CREATE OR REPLACE PROCEDURE btracker_single_processing(
    IN _block INT,
    IN _logs BOOLEAN
)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __start_ts timestamptz;
  __end_ts   timestamptz;
BEGIN
  PERFORM set_config('synchronous_commit', 'ON', false);

  IF _logs THEN
    RAISE NOTICE 'Btracker processing block: %...', _block;
    __start_ts := clock_timestamp();
  END IF;

  PERFORM process_block_range_balances(_block, _block);
  PERFORM process_block_range_withdrawals(_block, _block);
  PERFORM process_block_range_savings(_block, _block);
  PERFORM process_block_range_rewards(_block, _block);
  PERFORM process_block_range_delegations(_block, _block);
  PERFORM process_block_range_recurrent_transfers(_block, _block);
  PERFORM process_transfer_stats(_block, _block);
  PERFORM process_block_range_converts(_block, _block);
  PERFORM process_block_range_orders(_block, _block);
  PERFORM process_block_range_escrows(_block, _block);


  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'Btracker processed block % successfully in % s
    ', _block, (extract(epoch FROM __end_ts - __start_ts));
  END IF;
END
$$;

/**
 * raise_exception()
 * -----------------
 * Utility function to raise a custom exception.
 * Used by backend functions for input validation errors.
 *
 * @param TEXT  Error message to raise
 */
CREATE OR REPLACE FUNCTION raise_exception(TEXT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RAISE EXCEPTION '%', $1;
END
$$;

-- ============================================================================
-- APPLICATION ENTRY POINT
-- ============================================================================

/**
 * main()
 * ------
 * Application entry point that starts the block processing loop.
 * Called by scripts/process_blocks.sh to begin syncing.
 *
 * Behavior:
 * 1. Enables processing via allowProcessing()
 * 2. Enters infinite loop calling hive.app_next_iteration()
 * 3. Processes each block range via btracker_process_blocks()
 * 4. Exits when continueProcessing() returns FALSE
 *
 * To stop: Call stopProcessing() from another session and commit.
 *
 * @param _appContext    HAF context name
 * @param _maxBlockLimit Optional maximum block to process (for testing)
 */
CREATE OR REPLACE PROCEDURE main(
    IN _appContext hive.context_name,
    IN _maxBlockLimit INT = null
)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _blocks_range hive.blocks_range := (0,0);
BEGIN
  IF _maxBlockLimit != NULL THEN
    RAISE NOTICE 'Max block limit is specified as: %', _maxBlockLimit;
  END IF;

  PERFORM allowProcessing();

  RAISE NOTICE 'Last block processed by application: %', hive.app_get_current_block_num(_appContext);

  RAISE NOTICE 'Entering application main loop...';

  LOOP
    CALL hive.app_next_iteration(
      _appContext,
      _blocks_range,
      _override_max_batch => NULL,
      _limit => _maxBlockLimit);

    IF NOT continueProcessing() THEN
      ROLLBACK;
      RAISE NOTICE 'Exiting application main loop at processed block: %.', hive.app_get_current_block_num(_appContext);
      RETURN;
    END IF;

    IF _blocks_range IS NULL THEN
      RAISE INFO 'Waiting for next block...';
      CONTINUE;
    END IF;

    PERFORM btracker_process_blocks(_appContext, _blocks_range);
  END LOOP;

  ASSERT FALSE, 'Cannot reach this point';
END
$$;

-- ============================================================================
-- INDEX CREATION
-- ============================================================================

/**
 * create_btracker_indexes()
 * -------------------------
 * Create performance indexes for API queries.
 * Called once when transitioning from MASSIVE to LIVE processing.
 *
 * Indexes are deferred until after initial sync because:
 * 1. Building indexes on empty/small tables is fast
 * 2. Maintaining indexes during bulk inserts is slow
 * 3. Better to bulk load then index
 *
 * Index Purposes:
 * ---------------
 * Balance History:
 *   - idx_account_balance_history_account_seq_num_idx
 *     Cursor-based pagination by sequence number
 *   - idx_account_balance_history_account_block_num_idx
 *     Block range filtering (uses function-based index)
 *
 * Savings History:
 *   - idx_account_savings_history_account_seq_num_idx
 *     Cursor-based pagination by sequence number
 *   - idx_account_savings_history_account_block_num_idx
 *     Block range filtering
 *
 * Top Holders:
 *   - idx_account_balance_nai_balance_idx
 *     Order by balance DESC for top holder queries
 *   - idx_account_savings_nai_balance_idx
 *     Order by savings balance DESC
 *
 * Delegations:
 *   - idx_current_accounts_delegations_delegatee_idx
 *     Filter by delegatee for incoming delegations
 *
 * Recurrent Transfers:
 *   - idx_recurrent_transfers_to_account_idx
 *     Filter by to_account for incoming transfers
 */
CREATE OR REPLACE FUNCTION create_btracker_indexes()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  -- Balance history: pagination by sequence number (default ordering)
  CREATE UNIQUE INDEX IF NOT EXISTS idx_account_balance_history_account_seq_num_idx ON account_balance_history(account, nai, balance_seq_no);
  -- Savings history: pagination by sequence number (default ordering)
  CREATE UNIQUE INDEX IF NOT EXISTS idx_account_savings_history_account_seq_num_idx ON account_savings_history(account, nai, balance_seq_no);
  -- Balance history: block range filtering (function-based index on source_op)
  CREATE INDEX IF NOT EXISTS idx_account_balance_history_account_block_num_idx ON account_balance_history(account, nai, hafd.operation_id_to_block_num(source_op));
  -- Savings history: block range filtering (function-based index on source_op)
  CREATE INDEX IF NOT EXISTS idx_account_savings_history_account_block_num_idx ON account_savings_history(account, nai, hafd.operation_id_to_block_num(source_op));
  -- Top holders: order by balance descending
  CREATE INDEX IF NOT EXISTS idx_account_balance_nai_balance_idx ON current_account_balances(nai, balance DESC);
  -- Top savings holders: order by savings balance descending
  CREATE INDEX IF NOT EXISTS idx_account_savings_nai_balance_idx ON account_savings(nai,balance DESC);
  -- Delegations: filter by delegatee for incoming delegation queries
  CREATE INDEX IF NOT EXISTS idx_current_accounts_delegations_delegatee_idx ON current_accounts_delegations(delegatee);
  -- Recurrent transfers: filter by recipient for incoming transfer queries
  CREATE INDEX IF NOT EXISTS idx_recurrent_transfers_to_account_idx ON recurrent_transfers(to_account);
END
$$;

RESET ROLE;
