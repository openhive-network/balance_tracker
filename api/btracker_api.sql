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

CREATE OR REPLACE FUNCTION btracker_app.get_balance_for_coin_by_block(_account_name TEXT, _coin_type INT, _start_block BIGINT, _end_block BIGINT, _block_increment INT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{13, 21, 37}';
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
      FORMAT('ERROR: "_coin_type" must be one of %s!', __coin_type_arr));
  END IF;

  RETURN to_jsonb(result) FROM (
    SELECT
      json_agg(block_step) AS block,
      json_agg(balance) AS balance
    FROM (
      SELECT DISTINCT ON (block_step)
        block_step + 1 AS block_step,
        balance
      FROM (
        SELECT 
          block_step,
          max(balance) OVER (ORDER BY block_step) AS balance
        FROM (
          SELECT
            row_number() OVER (ORDER BY abh.source_op_block) AS id,
            (_block_increment * (abh.source_op_block / _block_increment)::BIGINT - 1)::BIGINT AS block_step,
            abh.balance::BIGINT AS balance
          FROM
            btracker_app.account_balance_history abh
          WHERE 
            abh.account = _account_name AND
            abh.nai = _coin_type AND
            abh.source_op_block >= _start_block AND
            abh.source_op_block <= _end_block
          ORDER BY abh.source_op_block ASC
        ) query
      ORDER BY block_step, id DESC
      ) last_block_values
    ) distinct_values
  ) result;
END
$$
;
