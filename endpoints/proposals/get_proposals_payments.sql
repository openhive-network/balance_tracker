SET ROLE btracker_owner;

/** openapi:paths
/proposals/payments:
  get:
    tags:
      - Proposals
    summary: HBD paid so far to many DHF proposals
    description: |
      Per proposal, the total HBD the Decentralized Hive Fund has paid it, the number of
      payments and the first and latest payment, summed from its `proposal_pay_operation`s.
      Meant for a proposal list: one call returns every listed proposal.

      Returns one entry per distinct requested id, in ascending id order. A proposal that
      has not been paid yet (or does not exist) gets a zero `total_paid`,
      `payment_count` 0 and nulls for `receiver` and the payment blocks and times.

      Every `proposal_pay_operation` in the chain''s history carries `proposal_id`,
      including the payments made by `steem.dao` before HF24, so every payment is
      attributed to its proposal.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_proposals_payments(''0,233,340'');`

      REST call example
      * `GET ''https://%1$s/balance-api/proposals/payments?proposal-ids=0,233,340''`
    operationId: btracker_endpoints.get_proposals_payments
    parameters:
      - in: query
        name: proposal-ids
        required: true
        schema:
          type: string
          x-sql-default-value: "NULL"
        description: Comma-separated proposal ids (at most 1000), e.g. `0,233,340`
    responses:
      '200':
        description: |
          Payment totals per requested proposal

          * Returns array of `btracker_backend.proposal_payment_totals`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.array_of_proposal_payment_totals'
            example: [
              {
                "proposal_id": 233,
                "receiver": "hbdstabilizer",
                "total_paid": {"nai": "@@000000013", "amount": "45689194336", "precision": 3},
                "payment_count": 5808,
                "first_payment_block": 68145659,
                "first_payment_time": "2022-09-23T00:35:03",
                "last_payment_block": 75104523,
                "last_payment_time": "2023-05-22T23:35:48"
              },
              {
                "proposal_id": 400,
                "receiver": null,
                "total_paid": {"nai": "@@000000013", "amount": "0", "precision": 3},
                "payment_count": 0,
                "first_payment_block": null,
                "first_payment_time": null,
                "last_payment_block": null,
                "last_payment_time": null
              }
            ]
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_proposals_payments;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_proposals_payments(
    "proposal-ids" TEXT = NULL
)
RETURNS SETOF btracker_backend.proposal_payment_totals 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
DECLARE
  _proposal_ids INT[] := btracker_backend.parse_proposal_ids("proposal-ids");
BEGIN
  -- New payments arrive every DHF maintenance period (hourly).
  PERFORM btracker_backend.set_history_cache_headers(NULL);

  RETURN QUERY SELECT * FROM btracker_backend.get_proposal_payment_totals(_proposal_ids);
END
$$;

RESET ROLE;
