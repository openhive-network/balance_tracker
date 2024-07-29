SET ROLE btracker_owner;

/** openapi:paths
/balance-for-coins/{account-name}:
  get:
    tags:
      - Balance-for-coins
    summary: Historical balance change
    description: |
      History of change of `coin-type` balance in given block range

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''blocktrades'');`

      * `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''initminer'');`

      REST call example
      * `GET https://{btracker-host}/%1$s/balance-for-coins/blocktrades`
      
      * `GET https://{btracker-host}/%1$s/balance-for-coins/initminer`
    operationId: btracker_endpoints.get_balance_for_coin_by_block
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
        name: from-block
        required: true
        schema:
          type: integer
          x-sql-datatype: BIGINT
        description: Lower limit of the block range
      - in: query
        name: to-block
        required: true
        schema:
          type: integer
          x-sql-datatype: BIGINT
        description: Upper limit of the block range
    responses:
      '200':
        description: |
          Balance change
        content:
          application/json:
            schema:
              type: string
              x-sql-datatype: JSONB
            example: ["blocktrade", "blocktrades"]
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_for_coin_by_block;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_for_coin_by_block(
    "account-name" TEXT,
    "coin-type" INT,
    "from-block" BIGINT,
    "to-block" BIGINT
)
RETURNS JSONB 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  __coin_type_arr INT[] = '{13, 21, 37}';
  __block_increment BIGINT;
BEGIN
  IF "from-block" >= "to-block" THEN
    SELECT raise_exception(
      'ERROR: "from-block" must be lower than "to-block"!');
  END IF;
  IF "coin-type" != ALL (__coin_type_arr) THEN
    SELECT raise_exception(
      FORMAT('ERROR: "coin-type" must be one of %s!', __coin_type_arr));
  END IF;

  __block_increment = (("to-block" - "from-block") / 1000)::BIGINT;

  IF "to-block" <= hive.app_get_irreversible_block() THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

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
                    ((((abh.source_op_block - 1 - "from-block") / __block_increment)::INT + 1) * __block_increment + "from-block")::BIGINT AS block_step,
                    abh.balance::BIGINT AS balance
                  FROM
                    account_balance_history abh JOIN
                    hive.accounts_view ha ON abh.account = ha.id
                  WHERE 
                    ha.name = "account-name" AND
                    abh.nai = "coin-type" AND
                    abh.source_op_block >= "from-block" AND
                    abh.source_op_block <= "to-block"
                  ORDER BY abh.source_op_block ASC
                ) hive_query
              ORDER BY block_step, id DESC
              ) last_block_values 
            ) distinct_values
            RIGHT JOIN (
              SELECT
                generate_series("from-block", "to-block" + __block_increment, __block_increment) AS block_step,
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
$$;

RESET ROLE;
