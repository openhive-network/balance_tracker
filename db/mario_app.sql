DROP SCHEMA IF EXISTS mario_app CASCADE;

CREATE SCHEMA IF NOT EXISTS mario_app;

CREATE OR REPLACE FUNCTION mario_app.define_schema()
RETURNS VOID
LANGUAGE 'plpgsql'
AS $$
BEGIN

RAISE NOTICE 'Attempting to create an application schema tables...';

CREATE TABLE IF NOT EXISTS mario_app.app_status
(
  continue_processing BOOLEAN NOT NULL,
  last_processed_block INT NOT NULL
);

INSERT INTO mario_app.app_status
(continue_processing, last_processed_block)
VALUES
(True, 0)
;

------------CUSTOM TABLES------------
---------------------------------------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS mario_app.h_transactions (
--     block_num integer NOT NULL,
--     trx_in_block smallint NOT NULL,
--     trx_hash bytea NOT NULL,
--     ref_block_num integer NOT NULL,
--     ref_block_prefix bigint NOT NULL,
--     expiration timestamp without time zone NOT NULL,
--     signature bytea DEFAULT NULL,
--     CONSTRAINT pk_hive_h_transactions PRIMARY KEY ( trx_hash, block_num )
-- )PARTITION BY HASH ( block_num );

-- ALTER TABLE mario_app.h_transactions ADD CONSTRAINT fk_1_hive_h_transactions FOREIGN KEY (block_num) REFERENCES hive.blocks (num);
---------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mario_app.r_transactions (
    block_num integer NOT NULL,
    trx_in_block smallint NOT NULL,
    trx_hash bytea NOT NULL,
    ref_block_num integer NOT NULL,
    ref_block_prefix bigint NOT NULL,
    expiration timestamp without time zone NOT NULL,
    signature bytea DEFAULT NULL,
    CONSTRAINT pk_hive_r_transactions PRIMARY KEY ( trx_hash, block_num )
)PARTITION BY RANGE ( block_num );

ALTER TABLE mario_app.r_transactions ADD CONSTRAINT fk_1_hive_r_transactions FOREIGN KEY (block_num) REFERENCES hive.blocks (num);
---------------------------------------------------------------------------------------------------------

--recreate role for reading data
IF (SELECT 1 FROM pg_roles WHERE rolname='mario_user') IS NOT NULL THEN
  DROP OWNED BY mario_user;
END IF;
DROP ROLE IF EXISTS mario_user;
CREATE ROLE mario_user LOGIN PASSWORD 'mario_user';
GRANT hive_applications_group TO mario_user;
GRANT USAGE ON SCHEMA mario_app to mario_user;
GRANT SELECT ON hive.blocks TO mario_user;

-- add ability for haf_admin to switch to mario_user role
GRANT mario_user TO haf_admin;

-- add mario_app schema owner
DROP ROLE IF EXISTS mario_owner;
CREATE ROLE mario_owner;
ALTER SCHEMA mario_app OWNER TO mario_owner;
END
$$
;
---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mario_app.create_h_partitions( table_name VARCHAR, modulus INTEGER)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    command text;
BEGIN
    IF EXISTS (select 1 from pg_partitioned_table p, pg_class c 
                where p.partrelid = c.oid
                and c.relname = table_name)
    THEN
		command := '';
        FOR i IN 1..modulus
        LOOP
            command := command || 'CREATE TABLE  '||table_name||'_'||(i-1)||' PARTITION OF mario_app.'||table_name||' FOR VALUES WITH ( modulus '||modulus||', remainder '||(i-1)||');';
        END LOOP;

		RAISE NOTICE '%', command;
        EXECUTE command;

    ELSE
        RAISE NOTICE 'Partitions can not be created.';
    END IF;
END $$;
---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mario_app.create_r_partitions( table_name VARCHAR, partition_cnt INTEGER, start INTEGER, nr_blocks INTEGER )
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
    command text;
BEGIN
    IF EXISTS (select 1 from pg_partitioned_table p, pg_class c 
                where p.partrelid = c.oid
                and c.relname = table_name)
    THEN
		command := '';
        FOR i IN 1..partition_cnt
        LOOP
            command := command || 'CREATE TABLE  '||table_name||'_'||(i-1)||' PARTITION OF mario_app.'||table_name||' FOR VALUES FROM ('||start + (i-1)*nr_blocks||') TO ('||start + (i)*nr_blocks||');';
        END LOOP;

		RAISE NOTICE '%', command;
        EXECUTE command;

    ELSE
        RAISE NOTICE 'Partitions can not be created.';
    END IF;
END $$;
---------------------------------------------------------------------------------
--- Helper function telling application main-loop to continue execution.
CREATE OR REPLACE FUNCTION mario_app.continueProcessing()
RETURNS BOOLEAN
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN continue_processing FROM mario_app.app_status LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.allowProcessing()
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE mario_app.app_status SET continue_processing = True;
END
$$
;

--- Helper function to be called from separate transaction (must be committed) to safely stop execution of the application.
CREATE OR REPLACE FUNCTION mario_app.stopProcessing()
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE mario_app.app_status SET continue_processing = False;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.storeLastProcessedBlock(IN _lastBlock INT)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  UPDATE mario_app.app_status SET last_processed_block = _lastBlock;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.lastProcessedBlock()
RETURNS INT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN last_processed_block FROM mario_app.app_status LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.process_block_range_data_c(in _from INT, in _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
BEGIN

  insert into mario_app.r_transactions
  select *
  from hive.transactions
  where block_num BETWEEN _from AND _to;

  -- insert into mario_app.h_transactions
  -- select *
  -- from hive.transactions
  -- where block_num BETWEEN _from AND _to;

END
$$
;

CREATE OR REPLACE PROCEDURE mario_app.do_massive_processing(IN _appContext VARCHAR, in _from INT, in _to INT, IN _step INT, INOUT _last_block INT)
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

    PERFORM mario_app.process_block_range_data_c(b, _last_block);

    COMMIT;

    RAISE NOTICE 'Block range: <%, %> processed successfully.', b, _last_block;

    EXIT WHEN NOT mario_app.continueProcessing();

  END LOOP;

  IF mario_app.continueProcessing() AND _last_block < _to THEN
    RAISE NOTICE 'Attempting to process a block range (rest): <%, %>', b, _last_block;
    --- Supplement last part of range if anything left.
    PERFORM mario_app.process_block_range_data_c(_last_block, _to);
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

CREATE OR REPLACE PROCEDURE mario_app.processBlock(in _block INT)
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  PERFORM mario_app.process_block_range_data_c(_block, _block);
  COMMIT; -- For single block processing we want to commit all changes for each one.
END
$$
;

/** Application entry point, which:
  - defines its data schema,
  - creates HAF application context,
  - starts application main-loop (which iterates infinitely). To stop it call `mario_app.stopProcessing();` from another session and commit its trasaction.
*/
CREATE OR REPLACE PROCEDURE mario_app.main(IN _appContext VARCHAR, IN _maxBlockLimit INT = 0)
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
    PERFORM mario_app.define_schema();
    --PERFORM mario_app.create_h_partitions('h_transactions', 100);
    PERFORM mario_app.create_r_partitions('r_transactions', 100, 0, 1000000);
    COMMIT;
  END IF;

  PERFORM mario_app.allowProcessing();
  COMMIT;

  SELECT mario_app.lastProcessedBlock() INTO __last_block;

  RAISE NOTICE 'Last block processed by application: %', __last_block;

  IF NOT hive.app_context_is_attached(_appContext) THEN
    PERFORM hive.app_context_attach(_appContext, __last_block);
  END IF;

  RAISE NOTICE 'Entering application main loop...';

  WHILE mario_app.continueProcessing() AND (_maxBlockLimit = 0 OR __last_block < _maxBlockLimit) LOOP
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
        CALL mario_app.do_massive_processing(_appContext, __next_block_range.first_block, __next_block_range.last_block, 100, __last_block);
      ELSE
        CALL mario_app.processBlock(__next_block_range.last_block);
        __last_block := __next_block_range.last_block;
      END IF;

    END IF;

  END LOOP;

  RAISE NOTICE 'Exiting application main loop at processed block: %.', __last_block;
  PERFORM mario_app.storeLastProcessedBlock(__last_block);

  COMMIT;
END
$$
;

CREATE OR REPLACE PROCEDURE mario_app.create_indexes()
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  --CREATE INDEX IF NOT EXISTS hive_h_transactions_block_num_trx_in_block_idx ON mario_app.h_transactions ( block_num, trx_in_block );
  CREATE INDEX IF NOT EXISTS hive_r_transactions_block_num_trx_in_block_idx ON mario_app.r_transactions ( block_num, trx_in_block );
END
$$
;
