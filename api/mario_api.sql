CREATE OR REPLACE FUNCTION mario_app.raise_exception(TEXT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
BEGIN
  RAISE EXCEPTION '%', $1;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.find_matching_accounts(_partial_account_name TEXT)
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
    LIMIT 50
  ) account_query;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.get_balance_for_coin_by_block(_account_name TEXT, _coin_type INT, _start_block BIGINT, _end_block BIGINT)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{13, 21, 37}';
  __block_increment BIGINT;
BEGIN
  IF _start_block >= _end_block THEN
    SELECT raise_exception(
      'ERROR: "_start_block" must be lower than "_end_block"!');
  END IF;
  IF _coin_type != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      FORMAT('ERROR: "_coin_type" must be one of %s!', __coin_type_arr));
  END IF;

  __block_increment = ((_end_block - _start_block) / 1000)::BIGINT;

  RETURN to_jsonb(result) FROM (
    SELECT
      json_agg(block_step) AS block,
      json_agg(CASE WHEN balance IS NULL THEN 0 ELSE balance END) AS balance
    FROM (
      SELECT
        block_step,
        first_value(balance) OVER (PARTITION BY value_partition_reverse) AS balance
      FROM (
        SELECT
          block_step,
          balance,
          SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY block_step DESC) AS value_partition_reverse
        FROM (
          SELECT
            block_step,
            first_value(balance) OVER (PARTITION BY value_partition) AS balance
          FROM (
            SELECT
              steps.block_step AS block_step,
              distinct_values.balance AS balance,
              SUM(CASE WHEN distinct_values.balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY steps.block_step) AS value_partition
            FROM (
              SELECT DISTINCT ON (block_step)
                block_step,
                balance
              FROM (
                SELECT 
                  block_step,
                  max(balance) OVER (ORDER BY block_step) AS balance
                FROM (
                  SELECT
                    row_number() OVER (ORDER BY abh.source_op_block) AS id,
                    ((((abh.source_op_block - 1 - _start_block) / __block_increment)::INT + 1) * __block_increment + _start_block)::BIGINT AS block_step,
                    abh.balance::BIGINT AS balance
                  FROM
                    mario_app.account_balance_history abh
                  WHERE 
                    abh.account = _account_name AND
                    abh.nai = _coin_type AND
                    abh.source_op_block >= _start_block AND
                    abh.source_op_block <= _end_block
                  ORDER BY abh.source_op_block ASC
                ) hive_query
              ORDER BY block_step, id DESC
              ) last_block_values 
            ) distinct_values
            RIGHT JOIN (
              SELECT
                generate_series(_start_block, _end_block + __block_increment, __block_increment) AS block_step,
                null AS balance
            ) steps
            ON distinct_values.block_step = steps.block_step
          ) join_tables
        ) fill_balance_bottom
      ) invert_value_partition
      ORDER BY block_step ASC
    ) fill_balance_top
  ) result;
END
$$
;

CREATE OR REPLACE FUNCTION mario_app.get_balance_for_coin_by_time(_account_name TEXT, _coin_type INT, _start_time TIMESTAMP, _end_time TIMESTAMP)
RETURNS TEXT
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{13, 21, 37}';
  __time_increment INTERVAL;
BEGIN
  IF _start_time >= _end_time THEN
    SELECT raise_exception(
      'ERROR: "_start_time" must be lower than "_end_time"!');
  END IF;
  IF _coin_type != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      FORMAT('ERROR: "_coin_type" must be one of %s!', __coin_type_arr));
  END IF;

  __time_increment = (_end_time - _start_time) / 1000;

  RETURN to_jsonb(result) FROM (
    SELECT
      json_agg(time_step) AS time,
      json_agg(CASE WHEN balance IS NULL THEN 0 ELSE balance END) AS balance
    FROM (
      SELECT
        time_step,
        first_value(balance) OVER (PARTITION BY value_partition_reverse) AS balance
      FROM (
        SELECT
          time_step,
          balance,
          SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY time_step DESC) AS value_partition_reverse
        FROM (
          SELECT
            time_step,
            first_value(balance) OVER (PARTITION BY value_partition) AS balance
          FROM (
            SELECT
              steps.time_step AS time_step,
              distinct_values.balance AS balance,
              SUM(CASE WHEN distinct_values.balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY steps.time_step) AS value_partition
            FROM (
              SELECT DISTINCT ON (time_step)
                time_step,
                balance
              FROM (
                SELECT
                  time_step,
                  max(balance) OVER (ORDER BY time_step) AS balance
                FROM (
                  SELECT
                    row_number() OVER (ORDER BY time_query.created_at) AS id,
                    (( (SELECT extract( EPOCH FROM ((time_query.created_at - '00:00:01'::TIME - _start_time)::TIME / (SELECT extract(EPOCH FROM __time_increment))) ))::INT + 1 ) * __time_increment + _start_time) AS time_step,
                    hive_query.balance AS balance
                  FROM (
                    SELECT
                      abh.source_op_block::BIGINT AS block,
                      abh.balance::BIGINT AS balance
                    FROM
                      mario_app.account_balance_history abh
                    WHERE
                      abh.account = _account_name AND
                      abh.nai = _coin_type
                    ORDER BY abh.source_op_block ASC
                  ) hive_query
                  LEFT JOIN (
                    SELECT
                      num AS block,
                      created_at::TIMESTAMP
                    FROM
                      hive.blocks
                  ) time_query
                  ON hive_query.block = time_query.block
                ) add_timestamps
              ORDER BY time_step, id DESC
              ) last_time_values
            ) distinct_values
            RIGHT JOIN (
              SELECT
                generate_series(_start_time, _end_time + __time_increment, __time_increment)::TIMESTAMP AS time_step,
                null AS balance
            ) steps
            ON distinct_values.time_step = steps.time_step
          ) join_tables
        ) fill_balance_bottom
      ) invert_value_partition
      ORDER BY time_step ASC
    ) fill_balance_top
  ) result;
END
$$
;
