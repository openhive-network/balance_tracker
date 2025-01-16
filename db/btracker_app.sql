-- noqa: disable=CP03

SET ROLE btracker_owner;

DO $$
  DECLARE __schema_name VARCHAR;
  DECLARE __context_table VARCHAR:='hive.';
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  __context_table:=__context_table || __schema_name;

  RAISE NOTICE 'balance_tracker will be installed in schema % with context %', __schema_name, __schema_name;

  IF hive.app_context_exists(__schema_name) THEN
      RAISE NOTICE 'Context % already exists, it means all tables are already created and data installing is skipped', __schema_name;
      RETURN;
  END IF;

  PERFORM hive.app_create_context(
    _name =>__schema_name,
    _schema => __schema_name,
    _is_forking => TRUE,
    _is_attached => FALSE
  );



RAISE NOTICE 'Attempting to create an application schema tables...';

CREATE TABLE IF NOT EXISTS app_status
(
  continue_processing BOOLEAN NOT NULL,
  withdraw_rate INT NOT NULL,
  start_delayed_vests BOOLEAN NOT NULL
);

INSERT INTO app_status
(continue_processing, withdraw_rate, start_delayed_vests)
VALUES
(True, 104, FALSE)
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
  RETURN continue_processing FROM app_status LIMIT 1;
END
$$;

CREATE OR REPLACE FUNCTION allowProcessing()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  UPDATE app_status SET continue_processing = True;
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
  UPDATE app_status SET continue_processing = False;
END
$$;

CREATE OR REPLACE PROCEDURE do_massive_processing(
    IN _appContext VARCHAR,
    IN _from INT,
    IN _to INT,
    IN _step INT,
    INOUT _last_block INT
)
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RAISE NOTICE 'Entering massive processing of block range: <%, %>...', _from, _to;
  RAISE NOTICE 'Detaching HAF application context...';
  PERFORM hive.app_context_detach(_appContext);

  --- You can do here also other things to speedup your app, i.e. disable constrains, remove indexes etc.

  FOR b IN _from .. _to BY _step LOOP
    _last_block := b + _step - 1;

    IF _last_block > _to THEN --- in case the _step is larger than range length
      _last_block := _to;
    END IF;

    RAISE NOTICE 'Attempting to process a block range: <%, %>', b, _last_block;

    PERFORM process_block_range_data_a(b, _last_block);
    PERFORM process_block_range_data_b(b, _last_block);


    PERFORM hive.app_set_current_block_num(_appContext, _last_block);

    COMMIT;

    RAISE NOTICE 'Block range: <%, %> processed successfully.', b, _last_block;

    EXIT WHEN NOT continueProcessing();

  END LOOP;

  RAISE NOTICE 'Attaching HAF application context at block: %.', _last_block;
  CALL hive.appproc_context_attach(_appContext);

 --- You should enable here all things previously disabled at begin of this function...

 RAISE NOTICE 'Leaving massive processing of block range: <%, %>...', _from, _to;
END
$$;

CREATE OR REPLACE PROCEDURE processBlock(IN _block INT)
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  PERFORM process_block_range_data_a(_block, _block);
  PERFORM process_block_range_data_b(_block, _block);
  COMMIT; -- For single block processing we want to commit all changes for each one.
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
    IN _appContext VARCHAR, IN _maxBlockLimit INT = 0
)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __last_block INT;
  __next_block_range hive.blocks_range;
  __original_commit_mode TEXT;
  __commit_mode_changed BOOLEAN := false;

BEGIN
  PERFORM allowProcessing();
  COMMIT;

  SELECT current_setting('synchronous_commit') into __original_commit_mode;

  SELECT hive.app_get_current_block_num(_appContext) INTO __last_block;
  RAISE NOTICE 'Last block processed by application: %', __last_block;

  IF NOT hive.app_context_is_attached(_appContext) THEN
    CALL hive.appproc_context_attach(_appContext);
  END IF;

  RAISE NOTICE 'Entering application main loop...';

  WHILE continueProcessing() AND (_maxBlockLimit = 0 OR __last_block < _maxBlockLimit) LOOP
    __next_block_range := hive.app_next_block(_appContext);

    IF __next_block_range IS NULL THEN
      RAISE WARNING 'Waiting for next block...';
    ELSE
      IF _maxBlockLimit != 0 and __next_block_range.first_block > _maxBlockLimit THEN
        __next_block_range.first_block  := _maxBlockLimit;
      END IF;

      IF _maxBlockLimit != 0 and __next_block_range.last_block > _maxBlockLimit THEN
        __next_block_range.last_block  := _maxBlockLimit;
      END IF;

      RAISE NOTICE 'Attempting to process block range: <%,%>', __next_block_range.first_block, __next_block_range.last_block;

      IF __next_block_range.first_block != __next_block_range.last_block THEN
        IF NOT __commit_mode_changed AND __original_commit_mode != 'OFF' THEN
          PERFORM set_config('synchronous_commit', 'OFF', false);
          __commit_mode_changed := true;
        END IF;
        CALL do_massive_processing(_appContext, __next_block_range.first_block, __next_block_range.last_block, 10000, __last_block);
      ELSE
        IF __commit_mode_changed THEN
          PERFORM set_config('synchronous_commit', __original_commit_mode, false);
          __commit_mode_changed := false;
        END IF;
        CALL processBlock(__next_block_range.last_block);
        __last_block := __next_block_range.last_block;
      END IF;

    END IF;
  END LOOP;

  RAISE NOTICE 'Exiting application main loop at processed block: %.', __last_block;
END
$$;

CREATE OR REPLACE FUNCTION create_btracker_indexes()
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
  CREATE INDEX idx_btracker_app_account_balance_history_nai ON account_balance_history(nai);
  CREATE INDEX idx_btracker_app_account_balance_history_account_nai ON account_balance_history(account, nai);

END
$$;

RESET ROLE;
