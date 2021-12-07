DROP SCHEMA IF EXISTS btracker_app CASCADE;

CREATE SCHEMA IF NOT EXISTS btracker_app;

CREATE OR REPLACE FUNCTION btracker_app.define_schema()
RETURNS VOID
LANGUAGE 'plpgsql'
AS $$
BEGIN

RAISE NOTICE 'Attempting to create an application schema tables...';

CREATE TABLE IF NOT EXISTS btracker_app.app_status
(
  continue_processing BOOLEAN NOT NULL,
  last_processed_block INT NOT NULL
);

INSERT INTO btracker_app.app_status
(continue_processing, last_processed_block)
VALUES
(True, 0)
;


CREATE TABLE IF NOT EXISTS btracker_app.current_account_balances
(
  account VARCHAR NOT NULL, -- Balance owner account
  nai     INT NOT NULL,     -- Balance type (currency)
  balance BIGINT NOT NULL,  -- Balance value (amount of held tokens)
  source_op BIGINT NOT NULL,-- The operation triggered last balance change
  source_op_block INT NOT NULL, -- Block containing the source operation

  CONSTRAINT pk_current_account_balances PRIMARY KEY (account, nai)
) INHERITS (hive.btracker_app);

CREATE TABLE IF NOT EXISTS btracker_app.account_balance_history
(
  account VARCHAR NOT NULL, -- Balance owner account
  nai     INT NOT NULL,     -- Balance type (currency)
  balance BIGINT NOT NULL,  -- Balance value after a change
  source_op BIGINT NOT NULL,-- The operation triggered given balance change
  source_op_block INT NOT NULL -- Block containing the source operation
  
  /** Because of bugs in blockchain at very begin, it was possible to make a transfer to self. See summon transfer in block 118570,
      like also `register` transfer in block 818601
      That's why constraint has been eliminated.
  */
  --CONSTRAINT pk_account_balance_history PRIMARY KEY (account, source_op_block, nai, source_op)
) INHERITS (hive.btracker_app);

--recreate role for reading data
DROP ROLE IF EXISTS api_user;
CREATE ROLE api_user;
GRANT USAGE ON SCHEMA btracker_app to api_user;
GRANT SELECT ON btracker_app.account_balance_history,btracker_app.current_account_balances TO api_user;

-- recreate role for connecting to db
DROP ROLE IF EXISTS admin;
CREATE ROLE admin NOINHERIT LOGIN PASSWORD 'admin';

-- add ability for admin to switch to api_user role
GRANT api_user TO admin;

END
$$
;

--- Helper function telling application main-loop to continue execution.
CREATE OR REPLACE FUNCTION btracker_app.continueProcessing()
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN continue_processing FROM btracker_app.app_status LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.allowProcessing()
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE btracker_app.app_status SET continue_processing = True;
END
$$
;

--- Helper function to be called from separate transaction (must be committed) to safely stop execution of the application.
CREATE OR REPLACE FUNCTION btracker_app.stopProcessing()
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE btracker_app.app_status SET continue_processing = False;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.storeLastProcessedBlock(IN _lastBlock INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE btracker_app.app_status SET last_processed_block = _lastBlock;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.lastProcessedBlock()
RETURNS INT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN last_processed_block FROM btracker_app.app_status LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_block_range_data_c(in _from INT, in _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql'
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  __balance_change RECORD;
  __current_balance BIGINT;
  __last_reported_block INT := 0;
BEGIN
FOR __balance_change IN
  WITH balance_impacting_ops AS
  (
    SELECT ot.id
    FROM hive.operation_types ot
    WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
  )
  SELECT bio.account_name AS account, bio.asset_symbol_nai AS nai, bio.amount as balance, ho.id AS source_op, ho.block_num AS source_op_block
  FROM hive.btracker_app_operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  JOIN balance_impacting_ops b ON ho.op_type_id = b.id
  JOIN LATERAL
  (
    SELECT * FROM hive.get_impacted_balances(ho.body)
  ) bio ON true
  WHERE ho.block_num BETWEEN _from AND _to
  ORDER BY ho.block_num, ho.id
LOOP
  INSERT INTO btracker_app.current_account_balances
    (account, nai, source_op, source_op_block, balance)
  SELECT __balance_change.account, __balance_change.nai, __balance_change.source_op, __balance_change.source_op_block, __balance_change.balance
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
    UPDATE SET balance = btracker_app.current_account_balances.balance + EXCLUDED.balance,
               source_op = EXCLUDED.source_op,
               source_op_block = EXCLUDED.source_op_block
  RETURNING balance into __current_balance;

  INSERT INTO btracker_app.account_balance_history
    (account, nai, source_op, source_op_block, balance)
  SELECT __balance_change.account, __balance_change.nai, __balance_change.source_op, __balance_change.source_op_block, COALESCE(__current_balance, 0)
  ;

--	if __balance_change.source_op_block = 199701 then
--	RAISE NOTICE 'Balance change data: account: %, amount change: %, source_op: %, source_block: %, current_balance: %', __balance_change.account, __balance_change.balance, __balance_change.source_op, __balance_change.source_op_block, COALESCE(__current_balance, 0);
--	end if;

  IF __balance_change.source_op_block % _report_step = 0 AND __last_reported_block != __balance_change.source_op_block THEN
    RAISE NOTICE 'Processed data for block: %', __balance_change.source_op_block;
    __last_reported_block := __balance_change.source_op_block;
  END IF;
END LOOP;

END
$$
;

CREATE OR REPLACE PROCEDURE btracker_app.do_massive_processing(IN _appContext VARCHAR, in _from INT, in _to INT, IN _step INT, INOUT _last_block INT)
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

    PERFORM btracker_app.process_block_range_data_c(b, _last_block);

    COMMIT;

    RAISE NOTICE 'Block range: <%, %> processed successfully.', b, _last_block;

    EXIT WHEN NOT btracker_app.continueProcessing();

  END LOOP;

  IF btracker_app.continueProcessing() AND _last_block < _to THEN
    RAISE NOTICE 'Attempting to process a block range (rest): <%, %>', b, _last_block;
    --- Supplement last part of range if anything left.
    PERFORM btracker_app.process_block_range_data_c(_last_block, _to);
    _last_block := _to;

    COMMIT;
    RAISE NOTICE 'Block range: <%, %> processed successfully.', b, _last_block;
  END IF;

  RAISE NOTICE 'Attaching HAF application context at block: %.', _last_block;
  PERFORM hive.app_context_attach(_appContext, _last_block);

 --- You should enable here all things previously disabled at begin of this function...

 RAISE NOTICE 'Leaving massive processing of block range: <%, %>...', _from, _to;
END
$$
;

CREATE OR REPLACE PROCEDURE btracker_app.processBlock(in _block INT)
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  PERFORM btracker_app.process_block_range_data_c(_block, _block);
  COMMIT; -- For single block processing we want to commit all changes for each one.
END
$$
;

/** Application entry point, which:
  - defines its data schema,
  - creates HAF application context,
  - starts application main-loop (which iterates infinitely). To stop it call `btracker_app.stopProcessing();` from another session and commit its trasaction.
*/
CREATE OR REPLACE PROCEDURE btracker_app.main(IN _appContext VARCHAR, IN _maxBlockLimit INT = 0)
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __last_block INT;
  __next_block_range hive.blocks_range;

BEGIN
  IF NOT hive.app_context_exists(_appContext) THEN
    RAISE NOTICE 'Attempting to create a HAF application context...';
    PERFORM hive.app_create_context(_appContext);
    PERFORM btracker_app.define_schema();
    COMMIT;
  END IF;

  PERFORM btracker_app.allowProcessing();
  COMMIT;

  SELECT btracker_app.lastProcessedBlock() INTO __last_block;

  RAISE NOTICE 'Last block processed by application: %', __last_block;

  IF NOT hive.app_context_is_attached(_appContext) THEN
    PERFORM hive.app_context_attach(_appContext, __last_block);
  END IF;

  RAISE NOTICE 'Entering application main loop...';

  WHILE btracker_app.continueProcessing() AND (_maxBlockLimit = 0 OR __last_block < _maxBlockLimit) LOOP
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
        CALL btracker_app.do_massive_processing(_appContext, __next_block_range.first_block, __next_block_range.last_block, 100, __last_block);
      ELSE
        CALL btracker_app.processBlock(__next_block_range.last_block);
        __last_block := __next_block_range.last_block;
      END IF;

    END IF;

  END LOOP;

  RAISE NOTICE 'Exiting application main loop at processed block: %.', __last_block;
  PERFORM btracker_app.storeLastProcessedBlock(__last_block);

  COMMIT;
END
$$
;

CREATE OR REPLACE FUNCTION raise_exception(TEXT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RAISE EXCEPTION '%', $1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.find_matching_accounts(_partial_account_name TEXT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE 
  __partial_account_name VARCHAR = LOWER(_partial_account_name || '%');
BEGIN
  RETURN json_agg(account_query.accounts
    ORDER BY
      account_query.name_lengths,
      account_query.accounts)
  FROM (
    SELECT DISTINCT ON (cab.account)
      cab.account AS accounts,
      LENGTH(cab.account) AS name_lengths
    FROM
      btracker_app.current_account_balances cab
    WHERE
      cab.account LIKE __partial_account_name
    ORDER BY
      accounts,
      name_lengths
    LIMIT 2000
  ) account_query;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_first_block(_account_name VARCHAR, _nai_code INT, _start_block BIGINT, _end_block BIGINT)
RETURNS BIGINT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN
    abh.source_op_block
  FROM
    btracker_app.account_balance_history abh
  WHERE 
    abh.account = _account_name AND
    abh.nai = _nai_code AND
    abh.source_op_block >= _start_block AND
    abh.source_op_block <= _end_block
  ORDER BY abh.source_op_block ASC
  LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_last_block(_account_name VARCHAR, _nai_code INT, _start_block BIGINT, _end_block BIGINT)
RETURNS BIGINT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN
    abh.source_op_block
  FROM
    btracker_app.account_balance_history abh
  WHERE 
    abh.account = _account_name AND
    abh.nai = _nai_code AND
    abh.source_op_block >= _start_block AND
    abh.source_op_block <= _end_block
  ORDER BY abh.source_op_block DESC
  LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_block_range(_account_name VARCHAR, _nai_code INT, _cur_block BIGINT, _next_block BIGINT)
RETURNS FLOAT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN
    abh.balance AS "balance"
  FROM
    btracker_app.account_balance_history abh
  WHERE 
    abh.account LIKE _account_name AND
    abh.nai = _nai_code AND
    abh.source_op_block > _cur_block AND
    abh.source_op_block <= _next_block
  ORDER BY abh.source_op_block DESC
  LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_coin_by_block(_account_name TEXT, _coin_type TEXT, _start_block BIGINT, _end_block BIGINT, _block_increment INT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __nai_code INT;
  __coin_type_arr TEXT[] = '{"steem", "hbd"}';
  __first_block BIGINT;
  __last_block BIGINT;
BEGIN
  IF _block_increment < (_end_block - _start_block) / 1000 THEN
    SELECT raise_exception(
      "ERROR: query is limited to 1000! Use higher '_block_increment' for this block range.");
  END IF;
  IF _coin_type != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      "ERROR: _coin_type must be 'steem' or 'hbd'!");
    ELSE
      -- TODO: check if not opposite
      __nai_code = CASE
      WHEN _coin_type = 'steem' THEN 21
      WHEN _coin_type = 'hbd' THEN 37
    END;
  END IF;

  SELECT get_first_block(_account_name, __nai_code, _start_block, _end_block) INTO __first_block;
  SELECT get_last_block(_account_name, __nai_code, _start_block, _end_block) INTO __last_block;

  RETURN json_agg(filled_values.filled_balance) FROM (
    SELECT
      first_value(balance) OVER (PARTITION BY value_partition) AS filled_balance
    FROM ( SELECT
      id,
      balance,
      SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY id) AS value_partition
      FROM ( WITH RECURSIVE incremental AS (
        SELECT
          0::BIGINT AS id,
          __first_block - _block_increment AS cur_block,
          __first_block AS next_block,
          0::FLOAT AS balance
        UNION ALL
        SELECT
          id + 1,
          cur_block + _block_increment,
          next_block + _block_increment,
          (SELECT get_balance_for_block_range(_account_name, __nai_code, cur_block, next_block))
        FROM incremental
        WHERE cur_block < __last_block
      )
      SELECT id, balance FROM incremental OFFSET 1
      ) incremental_query
    ) value_partition
  ) filled_values;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.add_them(in a integer, in b integer)RETURNS INT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN a + b;
END
$$
;
