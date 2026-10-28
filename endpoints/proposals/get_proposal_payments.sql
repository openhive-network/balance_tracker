SET ROLE btracker_owner;

/** openapi:paths
/proposals/{proposal-id}/payments:
  get:
    tags:
      - Proposals
    summary: Payment history of one DHF proposal
    description: |
      Paginated list of the payments (`proposal_pay_operation`) the Decentralized Hive
      Fund made to one proposal: block, timestamp and HBD amount of each. A proposal that
      has not been paid yet (or does not exist) has no payments.

      Every `proposal_pay_operation` in the chain''s history carries `proposal_id`,
      including the payments made by `steem.dao` before HF24, so every payment is
      attributed to its proposal.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_proposal_payments(233);`

      REST call example
      * `GET ''https://%1$s/balance-api/proposals/233/payments?page-size=50''`
    operationId: btracker_endpoints.get_proposal_payments
    parameters:
      - in: path
        name: proposal-id
        required: true
        schema:
          type: integer
        description: Id of the proposal
      - in: query
        name: page
        required: false
        schema:
          type: integer
          default: NULL
        description: |
          Return page on `page` number, default null due to reversed order of pages,
          the first page is the oldest,
          example: first call returns the newest page and total_pages is 100 - the newest page is number 100, next 99 etc.
      - in: query
        name: page-size
        required: false
        schema:
          type: integer
          default: 100
        description: Items per page (max 1000)
      - in: query
        name: direction
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.sort_direction'
          default: desc
        description: |
          Sort order:

           * `asc` - Ascending, from oldest to newest

           * `desc` - Descending, from newest to oldest
    responses:
      '200':
        description: |
          Paginated payment history of the proposal.

          * Returns `btracker_backend.proposal_payment_history`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.proposal_payment_history'
            example: {
              "total_operations": 5808,
              "total_pages": 59,
              "operations_result": [
                {
                  "block_num": 75104523,
                  "operation_id": "322571470066679940",
                  "amount": {"nai": "@@000000013", "amount": "8390554", "precision": 3},
                  "timestamp": "2023-05-22T23:35:48"
                }
              ]
            }
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_proposal_payments;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_proposal_payments(
    "proposal-id" INT,
    "page" INT = NULL,
    "page-size" INT = 100,
    "direction" btracker_backend.sort_direction = 'desc'
)
RETURNS btracker_backend.proposal_payment_history 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
BEGIN
  PERFORM btracker_backend.validate_proposal_id("proposal-id");
  PERFORM btracker_backend.validate_limit("page-size", 1000);
  PERFORM btracker_backend.validate_negative_limit("page-size");
  PERFORM btracker_backend.validate_negative_page("page");

  -- New payments arrive every DHF maintenance period (hourly).
  PERFORM btracker_backend.set_history_cache_headers(NULL);

  RETURN btracker_backend.get_proposal_payment_history(
    "proposal-id",
    "page",
    "page-size",
    "direction"
  );
END
$$;

RESET ROLE;
