CREATE OR REPLACE PROCEDURE btracker_app.create_api_user()
LANGUAGE 'plpgsql'
AS $$
BEGIN
  --recreate role for reading data
  DROP OWNED BY api_user;
  DROP ROLE IF EXISTS api_user;
  CREATE ROLE api_user;
  GRANT USAGE ON SCHEMA btracker_app to api_user;
  GRANT SELECT ON btracker_app.account_balance_history TO api_user;
  GRANT USAGE ON SCHEMA hive to api_user;
  GRANT SELECT ON hive.accounts TO api_user;

  -- recreate role for connecting to db
  DROP ROLE IF EXISTS admin;
  CREATE ROLE admin NOINHERIT LOGIN PASSWORD 'admin';

  -- add ability for admin to switch to api_user role
  GRANT api_user TO admin;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.raise_exception(TEXT)
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
    SELECT
      ha.name AS accounts,
      LENGTH(ha.name) AS name_lengths
    FROM
      hive.accounts ha
    WHERE
      ha.name LIKE __partial_account_name
    ORDER BY
      accounts,
      name_lengths
    LIMIT 2000
  ) account_query;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_first_balance(_account_name VARCHAR, _coin_type INT, _start_block BIGINT, _end_block BIGINT)
RETURNS BIGINT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RETURN
    abh.balance
  FROM
    btracker_app.account_balance_history abh
  WHERE 
    abh.account = _account_name AND
    abh.nai = _coin_type AND
    abh.source_op_block >= _start_block AND
    abh.source_op_block <= _end_block
  ORDER BY abh.source_op_block ASC
  LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_block_range(_account_name VARCHAR, _coin_type INT, _start_block BIGINT, _end_block BIGINT)
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
    abh.nai = _coin_type AND
    abh.source_op_block > _start_block AND
    abh.source_op_block <= _end_block
  ORDER BY abh.source_op_block DESC
  LIMIT 1;
END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_coin_by_block(_account_name TEXT, _coin_type INT, _start_block BIGINT, _end_block BIGINT, _block_increment INT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{21, 37}';
BEGIN
  IF _start_block >= _end_block THEN
    SELECT raise_exception(
      'ERROR: "_start_block" must be lower than "_end_block"!');
  END IF;
  IF _block_increment < (_end_block - _start_block) / 1000 THEN
    SELECT raise_exception(
      'ERROR: query is limited to 1000! Use higher "_block_increment" for this block range.');
  END IF;
  IF _coin_type != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      'ERROR: "_coin_type" must be "21" or "37"!');
  END IF;

  RETURN to_jsonb(result) FROM (
    SELECT
      json_agg(next_block) AS block,
      json_agg(filled_values.filled_balance) AS balance
    FROM (
      SELECT
        next_block,
        first_value(balance) OVER (PARTITION BY value_partition) AS filled_balance
      FROM ( SELECT
        id,
        balance,
        next_block,
        SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY id) AS value_partition
        FROM ( WITH RECURSIVE incremental AS (
          SELECT
            0::BIGINT AS id,
            _start_block - _block_increment AS cur_block,
            _start_block AS next_block,
            (SELECT get_first_balance(_account_name, _coin_type, _start_block, _end_block))::FLOAT AS balance
          UNION ALL
          SELECT
            id + 1,
            cur_block + _block_increment,
            next_block + _block_increment,
            (SELECT get_balance_for_block_range(_account_name, _coin_type, cur_block, next_block))
          FROM incremental
          WHERE cur_block < _end_block
        )
        SELECT id, balance, next_block FROM incremental
        ) incremental_query
      ) value_partition
      OFFSET 1
    ) filled_values
  ) result;

END
$$
;

/*
CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_coin_by_block(_account_name TEXT, _coin_type TEXT, _start_block BIGINT, _end_block BIGINT, _block_increment INT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  _coin_type INT;
  __coin_type_arr TEXT[] = '{"hive", "hbd"}';
BEGIN
  IF _coin_type != ALL (__coin_type_arr) THEN
    SELECT btracker_appraise_exception('ERROR: coin_type must be "hive" or "hbd"!');
    ELSE
      -- TODO: check if not opposite
      _coin_type = CASE
      WHEN _coin_type = 'hive' THEN 21
      WHEN _coin_type = 'hbd' THEN 37
    END;
  END IF;

  CREATE TEMP TABLE query_result AS (
    SELECT
      abh.source_op_block AS block_number,
      abh.balance AS account_balance
    FROM
      btracker_app.account_balance_history abh
    WHERE 
      abh.account = _account_name AND
      abh.nai = _coin_type AND
      abh.source_op_block >= _start_block AND
      abh.source_op_block <= _end_block
    ORDER BY abh.source_op_block ASC
  );

  RETURN json_agg(filled_values.filled_balance)
  FROM (
    SELECT
      id,
      first_value(balance) OVER (PARTITION BY value_partition) AS filled_balance
    FROM (
      SELECT
        id,
        balance,
        SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY id) AS value_partition
      FROM (
      WITH RECURSIVE incremental AS (
          SELECT
            0::BIGINT AS id,
            (SELECT MIN(block_number) - _block_increment FROM query_result) AS cur_block,
            (SELECT MIN(block_number) FROM query_result) AS next_block,
            0::FLOAT AS balance
        UNION ALL
          SELECT
            id + 1,
            cur_block + _block_increment,
            next_block + _block_increment,
            (SELECT account_balance FROM query_result WHERE block_number > cur_block AND block_number <= next_block ORDER BY block_number DESC LIMIT 1)
          FROM incremental
          WHERE cur_block < _end_block
      )
      SELECT id, balance FROM incremental OFFSET 1
      ) incremental_query
    ) value_partition
  ) filled_values
  ;
END
$$
;
*/