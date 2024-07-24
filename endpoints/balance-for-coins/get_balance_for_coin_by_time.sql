SET ROLE btracker_owner;

/** openapi:paths
/balance-for-coins/{account-name}/by-time:
  get:
    tags:
      - Balance-for-coins
    summary: Historical balance change
    description: |
      History of change of `coin-type` balance in time range

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''initminer'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/balance-for-coins/blocktrades/by-time`
      
      * `GET https://{btracker-host}/%1$s/balance-for-coins/initminer/by-time`
    operationId: btracker_endpoints.get_balance_for_coin_by_time
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
      - in: query
        name: coin-type
        required: true
        schema:
          type: integer
        description: |
          Coin types:

          * 13 - HBD

          * 21 - HIVE

          * 37 - VESTS
      - in: query
        name: start-date
        required: true
        schema:
          type: string
          format: date-time
        description: Lower limit of the time range
      - in: query
        name: end-date
        required: true
        schema:
          type: string
          format: date-time
        description: Upper limit of the time range
    responses:
      '200':
        description: |
          Balance change
        content:
          application/json:
            schema:
              type: string
            example: ["blocktrade", "blocktrades"]
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_for_coin_by_time;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_for_coin_by_time(
    "account-name" TEXT,
    "coin-type" INT,
    "start-date" TIMESTAMP,
    "end-date" TIMESTAMP
)
RETURNS TEXT 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{13, 21, 37}';
  __time_increment INTERVAL;
  _to INT := (SELECT bv.num FROM hive.blocks_view bv WHERE bv.created_at < "end-date" ORDER BY created_at DESC LIMIT 1)::INT;
BEGIN
  IF "start-date" >= "end-date" THEN
    SELECT raise_exception(
      'ERROR: "start-date" must be lower than "end-date"!');
  END IF;
  IF "coin-type" != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      FORMAT('ERROR: "coin-type" must be one of %s!', __coin_type_arr));
  END IF;

  IF _to <= hive.app_get_irreversible_block() THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  __time_increment = ("end-date" - "start-date") / 1000;

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
                    (( (SELECT extract( EPOCH FROM ((time_query.created_at - '00:00:01'::TIME - "start-date")::TIME / (SELECT extract(EPOCH FROM __time_increment))) ))::INT + 1 ) * __time_increment + "start-date") AS time_step,
                    hive_query.balance AS balance
                  FROM (
                    SELECT
                      abh.source_op_block::BIGINT AS block,
                      abh.balance::BIGINT AS balance
                    FROM
                      account_balance_history abh JOIN
                      hive.accounts_view ha ON abh.account = ha.id
                    WHERE
                      ha.name = "account-name" AND
                      abh.nai = "coin-type"
                    ORDER BY abh.source_op_block ASC
                  ) hive_query
                  LEFT JOIN (
                    SELECT
                      num AS block,
                      created_at::TIMESTAMP
                    FROM
                      hive.blocks_view
                  ) time_query
                  ON hive_query.block = time_query.block
                ) add_timestamps
              ORDER BY time_step, id DESC
              ) last_time_values
            ) distinct_values
            RIGHT JOIN (
              SELECT
                generate_series("start-date", "end-date" + __time_increment, __time_increment)::TIMESTAMP AS time_step,
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
$$;

RESET ROLE;
