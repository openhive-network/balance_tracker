SET ROLE btracker_owner;

-- =====================================================================
-- Create application schema and all tables
-- =====================================================================
DO $$
DECLARE
  __schema_name       VARCHAR;
  synchronization_stages hive.application_stages;
BEGIN
  -- determine the schema where we’re installing
  SHOW SEARCH_PATH INTO __schema_name;

  -- define our two sync stages
  synchronization_stages := ARRAY[
    hive.stage('MASSIVE_PROCESSING', 101, 10000, '20 seconds'),
    hive.live_stage()
  ]::hive.application_stages;

  RAISE NOTICE 'balance_tracker will be installed in schema % with context %',
               __schema_name, __schema_name;

  -- if we've already created the context, skip everything
  IF hive.app_context_exists(__schema_name) THEN
    RAISE NOTICE 'Context % already exists, skipping table creation', __schema_name;
    RETURN;
  END IF;

  -- create the HAF application context
  PERFORM hive.app_create_context(
    _name       => __schema_name,
    _schema     => __schema_name,
    _is_forking => TRUE,
    _stages     => synchronization_stages
  );

  --------------------------------------------------------------------
  -- Metadata tables
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS btracker_app_status (
    continue_processing BOOLEAN NOT NULL,
    is_indexes_created  BOOLEAN NOT NULL
  );
  INSERT INTO btracker_app_status (continue_processing, is_indexes_created)
    VALUES (TRUE, FALSE);

  CREATE TABLE IF NOT EXISTS version (
    git_hash TEXT
  );
  INSERT INTO version VALUES
    ('unspecified (generate and apply set_version_in_sql.pgsql)');

  --------------------------------------------------------------------
  -- ACCOUNT BALANCES
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS current_account_balances (
    account          INT     NOT NULL,
    nai              INT     NOT NULL,
    balance          BIGINT  NOT NULL,
    source_op        BIGINT  NOT NULL,
    source_op_block  INT     NOT NULL,
    CONSTRAINT pk_current_account_balances PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'current_account_balances',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS account_balance_history (
    account         INT     NOT NULL,
    nai             INT     NOT NULL,
    balance         BIGINT  NOT NULL,
    source_op       BIGINT  NOT NULL,
    source_op_block INT     NOT NULL
    -- no primary key, we want every historical row despite duplicates
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_balance_history',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS balance_history_by_month (
    account          INT       NOT NULL,
    nai              INT       NOT NULL,
    balance          BIGINT    NOT NULL,
    min_balance      BIGINT    NOT NULL,
    max_balance      BIGINT    NOT NULL,
    source_op        BIGINT    NOT NULL,
    source_op_block  INT       NOT NULL,
    updated_at       TIMESTAMP NOT NULL,
    CONSTRAINT pk_balance_history_by_month
      PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'balance_history_by_month',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS balance_history_by_day (
    account          INT       NOT NULL,
    nai              INT       NOT NULL,
    balance          BIGINT    NOT NULL,
    min_balance      BIGINT    NOT NULL,
    max_balance      BIGINT    NOT NULL,
    source_op        BIGINT    NOT NULL,
    source_op_block  INT       NOT NULL,
    updated_at       TIMESTAMP NOT NULL,
    CONSTRAINT pk_balance_history_by_day
      PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'balance_history_by_day',
                                 __schema_name);

  --------------------------------------------------------------------
  -- ACCOUNT REWARDS
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS account_rewards (
    account         INT     NOT NULL,
    nai             INT     NOT NULL,
    balance         BIGINT  NOT NULL,
    source_op       BIGINT  NOT NULL,
    source_op_block INT     NOT NULL,
    CONSTRAINT pk_account_rewards PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_rewards',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS account_info_rewards (
    account          INT     NOT NULL,
    posting_rewards  BIGINT DEFAULT 0,
    curation_rewards BIGINT DEFAULT 0,
    CONSTRAINT pk_account_info_rewards PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_info_rewards',
                                 __schema_name);

  --------------------------------------------------------------------
  -- ACCOUNT DELEGATIONS & RECURRENTS
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS current_accounts_delegations (
    delegator        INT     NOT NULL,
    delegatee        INT     NOT NULL,
    balance          BIGINT  NOT NULL,
    source_op        BIGINT  NOT NULL,
    source_op_block  INT     NOT NULL,
    CONSTRAINT pk_current_accounts_delegations
      PRIMARY KEY (delegator, delegatee)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'current_accounts_delegations',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS recurrent_transfers (
    from_account         INT     NOT NULL,
    to_account           INT     NOT NULL,
    transfer_id          INT     NOT NULL,
    nai                  INT     NOT NULL,
    amount               BIGINT  NOT NULL,
    consecutive_failures INT     NOT NULL,
    remaining_executions INT     NOT NULL,
    recurrence           INT     NOT NULL,
    memo                 TEXT    NOT NULL,
    source_op            BIGINT  NOT NULL,
    source_op_block      INT     NOT NULL,
    CONSTRAINT pk_recurrent_transfers
      PRIMARY KEY (from_account, to_account, transfer_id)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'recurrent_transfers',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS account_delegations (
    account         INT    NOT NULL,
    received_vests  BIGINT DEFAULT 0,
    delegated_vests BIGINT DEFAULT 0,
    CONSTRAINT pk_temp_vests PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_delegations',
                                 __schema_name);

  --------------------------------------------------------------------
  -- PENDING CONVERTS SUMMARY
  --------------------------------------------------------------------
  DROP TABLE IF EXISTS account_pending_converts;
  CREATE TABLE IF NOT EXISTS account_pending_converts AS
  WITH
    filled AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')::TEXT      AS account_name,
        (ov.body::jsonb->'value'->>'requestid')::BIGINT AS request_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name = 'hive::protocol::fill_convert_request_operation'
    ),
    pending AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')::TEXT AS account_name,
        (ov.body::jsonb->'value'->'amount'->>'nai') AS nai,
        ((ov.body::jsonb->'value'->'amount'->>'amount')::NUMERIC
          / power(
              10::NUMERIC,
              (ov.body::jsonb->'value'->'amount'->>'precision')::INT
            )
        ) AS amount
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name = 'hive::protocol::convert_operation'
        AND (ov.body::jsonb->'value'->>'requestid')::BIGINT
            NOT IN (SELECT request_id FROM filled)
    ),
    conv_summary AS (
      SELECT
        p.account_name,
        asset_map.asset,
        COUNT(p.amount)  AS request_count,
        SUM(p.amount)    AS total_amount
      FROM (VALUES
        ('@@000000013','HBD'),
        ('@@000000021','HIVE')
      ) AS asset_map(nai, asset)
      LEFT JOIN pending p ON p.nai = asset_map.nai
      GROUP BY p.account_name, asset_map.asset
    )
  SELECT * FROM conv_summary;
  PERFORM hive.app_register_table(__schema_name,
                                 'account_pending_converts',
                                 __schema_name);

  --------------------------------------------------------------------
  -- OPEN ORDERS SUMMARY
  --------------------------------------------------------------------
  DROP TABLE IF EXISTS account_open_orders_summary;
  CREATE TABLE IF NOT EXISTS account_open_orders_summary AS
  WITH
    filled_orders AS (
      SELECT DISTINCT
        (ov.body::jsonb->'value'->>'open_owner')::TEXT AS account_name,
        (v.id_text)::BIGINT                             AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      CROSS JOIN LATERAL (VALUES
        (ov.body::jsonb->'value'->>'open_orderid'),
        (ov.body::jsonb->'value'->>'current_orderid')
      ) AS v(id_text)
      WHERE ot.name IN (
        'hive::protocol::fill_order_operation',
        'hive::protocol::limit_order_fill_virtual_operation'
      )
        AND v.id_text IS NOT NULL
    ),
    cancelled_orders AS (
      SELECT DISTINCT
        (ov.body::jsonb->'value'->>'owner')::TEXT     AS account_name,
        (ov.body::jsonb->'value'->>'orderid')::BIGINT AS order_id
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name IN (
        'hive::protocol::limit_order_cancel_operation',
        'hive::protocol::limit_order_cancelled_operation'
      )
    ),
    open_creates AS (
      SELECT
        (ov.body::jsonb->'value'->>'owner')::TEXT               AS account_name,
        (ov.body::jsonb->'value'->>'orderid')::BIGINT           AS order_id,
        (ov.body::jsonb->'value'->'amount_to_sell'->>'nai')     AS nai,
        ((ov.body::jsonb->'value'->'amount_to_sell'->>'amount')::NUMERIC
           / power(
               10::NUMERIC,
               (ov.body::jsonb->'value'->'amount_to_sell'->>'precision')::INT
             )
        )                                               AS amount,
        (ov.body::jsonb->'value'->>'expiration')::timestamptz AS expiration
      FROM hive.operations_view ov
      JOIN hafd.operation_types ot ON ov.op_type_id = ot.id
      WHERE ot.name = 'hive::protocol::limit_order_create_operation'
        AND (ov.body::jsonb->'value'->>'expiration')::timestamptz > NOW()
        AND NOT EXISTS (
          SELECT 1 FROM filled_orders fo
          WHERE fo.order_id = (ov.body::jsonb->'value'->>'orderid')::BIGINT
        )
        AND NOT EXISTS (
          SELECT 1 FROM cancelled_orders co
          WHERE co.order_id = (ov.body::jsonb->'value'->>'orderid')::BIGINT
        )
    ),
    open_orders_summary AS (
      SELECT
        oc.account_name,
        COUNT(*) FILTER (WHERE am.asset = 'HBD')   AS open_orders_hbd_count,
        COUNT(*) FILTER (WHERE am.asset = 'HIVE')  AS open_orders_hive_count,
        SUM(oc.amount) FILTER (WHERE am.asset = 'HIVE') AS open_orders_hive_amount,
        SUM(oc.amount) FILTER (WHERE am.asset = 'HBD')  AS open_orders_hbd_amount
      FROM open_creates oc
      CROSS JOIN LATERAL (VALUES
        ('@@000000013','HBD'),
        ('@@000000021','HIVE')
      ) AS am(nai, asset)
      WHERE oc.nai = am.nai
      GROUP BY oc.account_name
    )
  SELECT * FROM open_orders_summary;
  PERFORM hive.app_register_table(__schema_name,
                                 'account_open_orders_summary',
                                 __schema_name);

  --------------------------------------------------------------------
  -- ACCOUNT WITHDRAWS & ROUTES
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS account_withdraws (
    account               INT     NOT NULL,
    vesting_withdraw_rate BIGINT  DEFAULT 0,
    to_withdraw           BIGINT  DEFAULT 0,
    withdrawn             BIGINT  DEFAULT 0,
    withdraw_routes       BIGINT  DEFAULT 0,
    delayed_vests         BIGINT  DEFAULT 0,
    source_op             BIGINT,
    CONSTRAINT pk_account_withdraws PRIMARY KEY (account)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_withdraws',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS account_routes (
    account     INT     NOT NULL,
    to_account  INT     NOT NULL,
    percent     INT     NOT NULL,
    source_op   BIGINT  NOT NULL,
    CONSTRAINT pk_account_routes PRIMARY KEY (account, to_account)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_routes',
                                 __schema_name);

  --------------------------------------------------------------------
  -- ACCOUNT SAVINGS
  --------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS account_savings (
    account                    INT     NOT NULL,
    nai                        INT     NOT NULL,
    saving_balance             BIGINT  DEFAULT 0,
    source_op                  BIGINT  NOT NULL,
    source_op_block            INT     NOT NULL,
    savings_withdraw_requests  INT     DEFAULT 0,
    CONSTRAINT pk_account_savings PRIMARY KEY (account, nai)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_savings',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS account_savings_history (
    account         INT     NOT NULL,
    nai             INT     NOT NULL,
    saving_balance  BIGINT  NOT NULL,
    source_op       BIGINT  NOT NULL,
    source_op_block INT     NOT NULL
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'account_savings_history',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS saving_history_by_month (
    account         INT       NOT NULL,
    nai             INT       NOT NULL,
    balance         BIGINT    NOT NULL,
    min_balance     BIGINT    NOT NULL,
    max_balance     BIGINT    NOT NULL,
    source_op       BIGINT    NOT NULL,
    source_op_block INT       NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    CONSTRAINT pk_saving_history_by_month
      PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'saving_history_by_month',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS saving_history_by_day (
    account         INT       NOT NULL,
    nai             INT       NOT NULL,
    balance         BIGINT    NOT NULL,
    min_balance     BIGINT    NOT NULL,
    max_balance     BIGINT    NOT NULL,
    source_op       BIGINT    NOT NULL,
    source_op_block INT       NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    CONSTRAINT pk_saving_history_by_day
      PRIMARY KEY (account, nai, updated_at)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'saving_history_by_day',
                                 __schema_name);

  CREATE TABLE IF NOT EXISTS transfer_saving_id (
    account     INT     NOT NULL,
    nai         INT     NOT NULL,
    balance     BIGINT  NOT NULL,
    request_id  BIGINT  NOT NULL,
    CONSTRAINT pk_transfer_saving_id
      PRIMARY KEY (account, request_id)
  );
  PERFORM hive.app_register_table(__schema_name,
                                 'transfer_saving_id',
                                 __schema_name);

END
$$;

-- =====================================================================
-- Helper functions & procedures
-- =====================================================================

-- continue flag
CREATE OR REPLACE FUNCTION continueProcessing()
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN continue_processing
  FROM btracker_app_status
  LIMIT 1;
END;
$$;

-- set continue = true
CREATE OR REPLACE FUNCTION allowProcessing()
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
  UPDATE btracker_app_status
    SET continue_processing = TRUE;
END;
$$;

-- set continue = false
CREATE OR REPLACE FUNCTION stopProcessing()
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
  UPDATE btracker_app_status
    SET continue_processing = FALSE;
END;
$$;

-- check if initial indexes exist
CREATE OR REPLACE FUNCTION isIndexesCreated()
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN pg_class c ON i.indexrelid = c.oid
    WHERE c.relname = 'idx_account_balance_history_account_source_op_idx'
  );
END;
$$;

-- main dispatcher: choose massive vs. single
CREATE OR REPLACE FUNCTION btracker_process_blocks(
    _context_name hive.context_name,
    _block_range   hive.blocks_range,
    _logs          BOOLEAN DEFAULT TRUE
)
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
  IF hive.get_current_stage_name(_context_name) = 'MASSIVE_PROCESSING' THEN
    CALL btracker_massive_processing(
      _block_range.first_block,
      _block_range.last_block,
      _logs
    );
    RETURN;
  END IF;

  IF NOT isIndexesCreated() THEN
    PERFORM create_btracker_indexes();
  END IF;

  CALL btracker_single_processing(_block_range.first_block, _logs);
END;
$$;

-- bulk initial load
CREATE OR REPLACE PROCEDURE btracker_massive_processing(
    IN _from INT,
    IN _to   INT,
    IN _logs BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
  __start_ts        timestamptz;
  __end_ts          timestamptz;
  __hardfork_23_blk INT := (
    SELECT block_num
    FROM hafd.applied_hardforks
    WHERE hardfork_num = 23
  );
BEGIN
  PERFORM set_config('synchronous_commit','OFF',false);

  IF _logs THEN
    RAISE NOTICE 'Processing MASSIVE range <% , %>', _from, _to;
    __start_ts := clock_timestamp();
  END IF;

  IF __hardfork_23_blk BETWEEN _from AND _to THEN
    PERFORM process_block_range_balances(_from,        __hardfork_23_blk);
    PERFORM process_block_range_withdrawals(_from,     __hardfork_23_blk);
    PERFORM process_block_range_savings(_from,         __hardfork_23_blk);
    PERFORM process_block_range_rewards(_from,         __hardfork_23_blk);
    PERFORM process_block_range_delegations(_from,     __hardfork_23_blk);
    PERFORM process_block_range_recurrent_transfers(_from, __hardfork_23_blk);

    PERFORM btracker_backend.process_hf_23(__hardfork_23_blk);
    RAISE NOTICE 'Hardfork 23 processed at block %', __hardfork_23_blk;

    IF __hardfork_23_blk < _to THEN
      PERFORM process_block_range_balances(__hardfork_23_blk+1, _to);
      PERFORM process_block_range_withdrawals(__hardfork_23_blk+1, _to);
      PERFORM process_block_range_savings(__hardfork_23_blk+1, _to);
      PERFORM process_block_range_rewards(__hardfork_23_blk+1, _to);
      PERFORM process_block_range_delegations(__hardfork_23_blk+1, _to);
      PERFORM process_block_range_recurrent_transfers(__hardfork_23_blk+1, _to);
    END IF;

  ELSE
    PERFORM process_block_range_balances(_from, _to);
    PERFORM process_block_range_withdrawals(_from, _to);
    PERFORM process_block_range_savings(_from, _to);
    PERFORM process_block_range_rewards(_from, _to);
    PERFORM process_block_range_delegations(_from, _to);
    PERFORM process_block_range_recurrent_transfers(_from, _to);
  END IF;

  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'MASSIVE range processed in % s',
      extract(epoch FROM __end_ts - __start_ts);
  END IF;
END;
$$;

-- incremental, per-block processing
CREATE OR REPLACE PROCEDURE btracker_single_processing(
    IN _block INT,
    IN _logs  BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
  __start_ts timestamptz;
  __end_ts   timestamptz;
BEGIN
  PERFORM set_config('synchronous_commit','ON',false);

  IF _logs THEN
    RAISE NOTICE 'Processing single block %...', _block;
    __start_ts := clock_timestamp();
  END IF;

  PERFORM process_block_range_balances(_block, _block);
  PERFORM process_block_range_withdrawals(_block, _block);
  PERFORM process_block_range_savings(_block, _block);
  PERFORM process_block_range_rewards(_block, _block);
  PERFORM process_block_range_delegations(_block, _block);
  PERFORM process_block_range_recurrent_transfers(_block, _block);

  IF _logs THEN
    __end_ts := clock_timestamp();
    RAISE NOTICE 'Block % processed in % s',
      _block, extract(epoch FROM __end_ts - __start_ts);
  END IF;
END;
$$;

-- raise arbitrary exception from SQL
CREATE OR REPLACE FUNCTION raise_exception(TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION '%', $1;
END;
$$;

-- application entry point
CREATE OR REPLACE PROCEDURE main(
    IN _appContext    hive.context_name,
    IN _maxBlockLimit INT = NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  _blocks_range hive.blocks_range := (0,0);
BEGIN
  IF _maxBlockLimit IS NOT NULL THEN
    RAISE NOTICE 'Max block limit: %', _maxBlockLimit;
  END IF;

  PERFORM allowProcessing();
  RAISE NOTICE 'Starting from block %',
               hive.app_get_current_block_num(_appContext);

  LOOP
    CALL hive.app_next_iteration(
      _appContext,
      _blocks_range,
      _override_max_batch => NULL,
      _limit             => _maxBlockLimit
    );

    IF NOT continueProcessing() THEN
      ROLLBACK;
      RAISE NOTICE 'Stopping at block %',
                   hive.app_get_current_block_num(_appContext);
      RETURN;
    END IF;

    IF _blocks_range IS NULL THEN
      RAISE INFO 'Waiting for next block…';
      CONTINUE;
    END IF;

    PERFORM btracker_process_blocks(_appContext, _blocks_range);
  END LOOP;
END;
$$;

-- create secondary indexes after massive load
CREATE OR REPLACE FUNCTION create_btracker_indexes()
RETURNS VOID
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
  CREATE INDEX IF NOT EXISTS
    idx_account_balance_history_account_source_op_idx
    ON account_balance_history(account, nai, source_op DESC);

  CREATE INDEX IF NOT EXISTS
    idx_account_savings_history_account_source_op_idx
    ON account_savings_history(account, nai, source_op DESC);

  CREATE INDEX IF NOT EXISTS
    idx_current_accounts_delegations_delegatee_idx
    ON current_accounts_delegations(delegatee);

  CREATE INDEX IF NOT EXISTS
    idx_recurrent_transfers_to_account_idx
    ON recurrent_transfers(to_account);
END;
$$;

RESET ROLE;
