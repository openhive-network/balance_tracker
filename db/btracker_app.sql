-- noqa: disable=CP03

SET ROLE btracker_owner;

DO $$
  DECLARE __schema_name VARCHAR;
  DECLARE __context_table VARCHAR:='hive.';
  synchronization_stages hive.application_stages;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  __context_table:=__context_table || __schema_name;

  synchronization_stages := ARRAY[( 'MASSIVE_PROCESSING', 101, 10000 ), hive.live_stage()]::hive.application_stages;

  RAISE NOTICE 'balance_tracker will be installed in schema % with context %', __schema_name, __schema_name;

  IF hive.app_context_exists(__schema_name) THEN
      RAISE NOTICE 'Context % already exists, it means all tables are already created and data installing is skipped', __schema_name;
      RETURN;
  END IF;

  PERFORM hive.app_create_context(
    _name =>__schema_name,
    _schema => __schema_name,
    _is_forking => TRUE,
    _is_attached => FALSE,
    _stages => synchronization_stages
  );



RAISE NOTICE 'Attempting to create an application schema tables...';

CREATE TABLE IF NOT EXISTS btracker_app_status
(
  continue_processing BOOLEAN NOT NULL,
  withdraw_rate INT NOT NULL,
  start_delayed_vests BOOLEAN NOT NULL,
  is_indexes_created BOOLEAN NOT NULL
);

INSERT INTO btracker_app_status
(continue_processing, withdraw_rate, start_delayed_vests, is_indexes_created)
VALUES
(True, 104, False, False)
;

--ACCOUNT BALANCES

CREATE TABLE IF NOT EXISTS current_account_balances
(
  account INT NOT NULL, -- Balance owner account
  nai     INT NOT NULL,     -- Balance type (currency)
  balance BIGINT NOT NULL,  -- Balance value (amount of held tokens)
  source_op BIGINT NOT NULL,-- The operation triggered last balance change
  source_op_block INT NOT NULL, -- Block containing the source operation

  CONSTRAINT pk_current_account_balances PRIMARY KEY (account, nai)
);

PERFORM hive.app_register_table( __schema_name, 'current_account_balances', __schema_name );

CREATE TABLE IF NOT EXISTS account_balance_history
(
  account INT NOT NULL, -- Balance owner account
  nai     INT NOT NULL,     -- Balance type (currency)
  balance BIGINT NOT NULL,  -- Balance value after a change
  source_op BIGINT NOT NULL,-- The operation triggered given balance change
  source_op_block INT NOT NULL -- Block containing the source operation
  
  /** Because of bugs in blockchain at very begin, it was possible to make a transfer to self. See summon transfer in block 118570,
      like also `register` transfer in block 818601
      That's why constraint has been eliminated.
  */
  --CONSTRAINT pk_account_balance_history PRIMARY KEY (account, source_op_block, nai, source_op)
);

PERFORM hive.app_register_table( __schema_name, 'account_balance_history', __schema_name );

--ACCOUNT REWARDS

CREATE TABLE IF NOT EXISTS account_rewards
(
  account INT NOT NULL, -- Balance owner account
  nai     INT NOT NULL,     -- Balance type (currency)
  balance BIGINT NOT NULL,  -- Balance value (amount of held tokens)
  source_op BIGINT NOT NULL,-- The operation triggered last balance change
  source_op_block INT NOT NULL, -- Block containing the source operation

  CONSTRAINT pk_account_rewards PRIMARY KEY (account, nai)
);

PERFORM hive.app_register_table( __schema_name, 'account_rewards', __schema_name );

CREATE TABLE IF NOT EXISTS account_info_rewards
(
  account INT NOT NULL, 
  posting_rewards BIGINT DEFAULT 0,
  curation_rewards  BIGINT DEFAULT 0, 

  CONSTRAINT pk_account_info_rewards PRIMARY KEY (account)
);

  PERFORM hive.app_register_table( __schema_name, 'account_info_rewards', __schema_name );

--ACCOUNT DELEGATIONS

CREATE TABLE IF NOT EXISTS current_accounts_delegations
(
  delegator INT NOT NULL,
  delegatee INT NOT NULL,     
  balance BIGINT NOT NULL,
  source_op BIGINT NOT NULL,
  source_op_block INT NOT NULL, 

  CONSTRAINT pk_current_accounts_delegations PRIMARY KEY (delegator, delegatee)
);
PERFORM hive.app_register_table( __schema_name, 'current_accounts_delegations', __schema_name );

CREATE TABLE IF NOT EXISTS account_delegations
(
  account INT NOT NULL,
  received_vests BIGINT DEFAULT 0,     
  delegated_vests BIGINT DEFAULT 0,         

  CONSTRAINT pk_temp_vests PRIMARY KEY (account)
);
PERFORM hive.app_register_table( __schema_name, 'account_delegations', __schema_name );

--ACCOUNT WITHDRAWS

CREATE TABLE IF NOT EXISTS account_withdraws
(
  account INT NOT NULL,
  vesting_withdraw_rate BIGINT DEFAULT 0,     
  to_withdraw BIGINT DEFAULT 0,
  withdrawn BIGINT DEFAULT 0,          
  withdraw_routes BIGINT DEFAULT 0,
  delayed_vests BIGINT DEFAULT 0,  

  CONSTRAINT pk_account_withdraws PRIMARY KEY (account)
);
PERFORM hive.app_register_table( __schema_name, 'account_withdraws', __schema_name );

CREATE TABLE IF NOT EXISTS account_routes
(
  account INT NOT NULL,
  to_account INT NOT NULL,     
  percent INT NOT NULL,
    
  CONSTRAINT pk_account_routes PRIMARY KEY (account, to_account)
);
PERFORM hive.app_register_table( __schema_name, 'account_routes', __schema_name );

--ACCOUNT SAVINGS

CREATE TABLE IF NOT EXISTS account_savings
(
  account INT NOT NULL,
  nai     INT NOT NULL, 
  saving_balance BIGINT DEFAULT 0,         
  source_op BIGINT NOT NULL,
  source_op_block INT NOT NULL,
  savings_withdraw_requests INT DEFAULT 0,

  CONSTRAINT pk_account_savings PRIMARY KEY (account, nai)
);
PERFORM hive.app_register_table( __schema_name, 'account_savings', __schema_name );

CREATE TABLE IF NOT EXISTS transfer_saving_id
(
  account INT NOT NULL,
  nai     INT NOT NULL, 
  balance BIGINT NOT NULL,
  request_id  BIGINT NOT NULL, 

  CONSTRAINT pk_transfer_saving_id PRIMARY KEY (account, request_id)
);
PERFORM hive.app_register_table( __schema_name, 'transfer_saving_id', __schema_name );

END
$$;

--- Helper function telling application main-loop to continue execution.
CREATE OR REPLACE FUNCTION continueProcessing()
RETURNS BOOLEAN
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN continue_processing FROM btracker_app_status LIMIT 1;
END
$$;

CREATE OR REPLACE FUNCTION allowProcessing()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE btracker_app_status SET continue_processing = True;
END
$$;

/** Helper function to be called from separate transaction (must be committed)
    to safely stop execution of the application.
**/
CREATE OR REPLACE FUNCTION stopProcessing()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE btracker_app_status SET continue_processing = False;
END
$$;

CREATE OR REPLACE FUNCTION updateIndexesCreated(_is_indexes_created BOOLEAN)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE btracker_app_status SET is_indexes_created = _is_indexes_created;
END
$$;

CREATE OR REPLACE FUNCTION isIndexesCreated()
RETURNS BOOLEAN
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN is_indexes_created FROM btracker_app_status LIMIT 1;
END
$$;

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
  ELSE
    IF NOT isIndexesCreated() THEN
      PERFORM create_btracker_indexes();
      PERFORM updateIndexesCreated(true);
    END IF;
    CALL btracker_single_processing(_block_range.first_block, _logs);
  END IF;
END
$$;

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
BEGIN
  PERFORM set_config('synchronous_commit', 'OFF', false);

  IF _logs THEN
    RAISE NOTICE 'Btracker is attempting to process a block range: <%, %>', _from, _to;
    __start_ts := clock_timestamp();
  END IF;

  PERFORM btracker_block_range_data_a(_from, _to);
  PERFORM btracker_block_range_data_b(_from, _to);

  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'Btracker processed block range: <%, %> successfully in % s
    ', _from, _to, (extract(epoch FROM __end_ts - __start_ts));
  END IF;
END
$$;

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

  PERFORM btracker_block_range_data_a(_block, _block);
  PERFORM btracker_block_range_data_b(_block, _block);

  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'Btracker processed block % successfully in % s
    ', _block, (extract(epoch FROM __end_ts - __start_ts));
  END IF;
END
$$;

/** Application entry point, which:
  - defines its data schema,
  - creates HAF application context,
  - starts application main-loop (which iterates infinitely).
    To stop it call `stopProcessing();`
    from another session and commit its trasaction.
*/
CREATE OR REPLACE PROCEDURE main(
    IN _appContext hive.context_name,
    IN _maxBlockLimit INT = NULL
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
      RETURN;
    END IF;

    IF _blocks_range IS NULL THEN
      RAISE WARNING 'Waiting for next block...';
      CONTINUE;
    END IF;

    PERFORM btracker_process_blocks(_appContext, _blocks_range);
  END LOOP;


  RAISE NOTICE 'Exiting application main loop at processed block: %.', hive.app_get_current_block_num(_appContext);
END
$$;

CREATE OR REPLACE FUNCTION create_btracker_indexes()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  CREATE INDEX IF NOT EXISTS idx_btracker_app_account_balance_history_nai ON account_balance_history(nai);
  CREATE INDEX IF NOT EXISTS idx_btracker_app_account_balance_history_account_nai ON account_balance_history(account, nai);
END
$$;

RESET ROLE;
