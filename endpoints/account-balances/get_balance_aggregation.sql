SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/aggregated-balance:
  get:
    tags:
      - Accounts
    summary: Historical balance change
    description: |
      History of change of `coin-type` balance in given block range

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_aggregation(''blocktrades'', ''HIVE'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/aggregated-balance?coin-type=VESTS''`
    operationId: btracker_endpoints.get_balance_aggregation
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
          $ref: '#/components/schemas/btracker_endpoints.nai_type'
        description: |
          Coin types:

          * HBD

          * HIVE

          * VESTS
      - in: query
        name: aggregation-step
        required: true
        schema:
          type: integer
          default: NULL
        description: |
          Sets the aggregation step size for grouping account balance changes, specified in units of block numbers. 
          For example, a value of 1000000 groups balance changes into intervals of 1 million blocks.
    responses:
      '200':
        description: |
          Balance change
        content:
          application/json:
            schema:
              type: string
              x-sql-datatype: JSON
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_aggregation;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_aggregation(
    "account-name" TEXT,
    "coin-type" btracker_endpoints.nai_type,
    "aggregation-step" INT
)
RETURNS JSON 
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
SET jit = OFF
AS
$$
DECLARE
  _coin_type INT := (CASE WHEN "coin-type" = 'HBD' THEN 13 WHEN "coin-type" = 'HIVE' THEN 21 ELSE 37 END);
  _account_id INT = (SELECT av.id FROM hive.accounts_view av WHERE av.name = "account-name");
BEGIN
  RETURN  (
    SELECT to_json(array_agg(row)) FROM (
      WITH block_intervals AS (
        SELECT
          source_op_block / "aggregation-step" AS block_interval,
          MAX(balance) AS max_balance,
          MIN(balance) AS min_balance,
          AVG(balance)::BIGINT AS avg_balance
        FROM
          btracker_app.account_balance_history
        WHERE
          account = _account_id AND
          nai = _coin_type
        GROUP BY
          block_interval
      )
      SELECT
        block_interval * "aggregation-step" + 1 AS start_block,
        (block_interval + 1) * "aggregation-step" AS end_block,
        max_balance,
        min_balance,
        avg_balance
      FROM 
        block_intervals
      ORDER BY
        block_interval
    ) row
  ); 
END
$$;

RESET ROLE;
