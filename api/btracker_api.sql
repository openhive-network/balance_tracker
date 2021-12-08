CREATE OR REPLACE PROCEDURE btracker_app.create_api_user()
LANGUAGE 'plpgsql'
AS $$
BEGIN
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
