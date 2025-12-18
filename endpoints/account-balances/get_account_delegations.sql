SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/delegations:
  get:
    tags:
      - Accounts
    summary: Account delegations
    description: |
      List of incoming and outgoing delegations

      SQL example
      * `SELECT * FROM btracker_endpoints.get_balance_delegations(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/delegations''`
    operationId: btracker_endpoints.get_balance_delegations
    parameters:
      - in: path
        name: account-name
        required: true
        schema:
          type: string
        description: Name of the account
    responses:
      '200':
        description: |
          Incoming and outgoing delegations

          * Returns `btracker_backend.delegations`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.delegations'
            example: {
                  "outgoing_delegations": [],
                  "incoming_delegations": []
                }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_balance_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_balance_delegations(
    "account-name" TEXT
)
RETURNS btracker_backend.delegations 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
/*
================================================================================
ENDPOINT: get_balance_delegations
================================================================================
PURPOSE:
  Returns all active VESTS delegations for an account, split into two arrays:
  - Incoming delegations (VESTS received from other accounts)
  - Outgoing delegations (VESTS delegated to other accounts)

ARCHITECTURE:
  1. Resolves account name to account_id (404 on missing)
  2. Queries current_accounts_delegations table via backend helpers
  3. Aggregates results into typed arrays, sorted by amount descending

DATA FLOW:
  current_accounts_delegations -> incoming_delegations() / outgoing_delegations()
                               -> array_agg with ORDER BY amount DESC
                               -> btracker_backend.delegations composite

DELEGATION MECHANICS:
  - Delegations transfer voting power without transferring ownership
  - Delegator retains the VESTS but loses voting influence
  - Delegatee gains voting power but cannot sell/transfer the VESTS
  - Undelegating has a 5-day return delay (tracked separately)

PLANNER HINTS:
  - from_collapse_limit = 16: Allow planner to consider more join orderings
  - join_collapse_limit = 16: Higher threshold for explicit JOIN optimization
  - jit = OFF: Disable JIT compilation (faster for simple queries)

PERFORMANCE NOTES:
  - Backend helpers perform account name lookups via hive.accounts_view
  - Results sorted by amount DESC to show largest delegations first
  - Empty arrays returned as '{}' (not NULL) for consistent JSON output

RETURN TYPE: btracker_backend.delegations
  - outgoing_delegations[]: Array of (delegatee, amount, operation_id, block_num)
  - incoming_delegations[]: Array of (delegator, amount, operation_id, block_num)
================================================================================
*/
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _incoming_delegations btracker_backend.incoming_delegations[];
  _outgoing_delegations btracker_backend.outgoing_delegations[];
BEGIN
  -- Set short cache - delegations can change frequently
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  ---------------------------------------------------------------------------
  -- INCOMING DELEGATIONS
  -- Query: Who has delegated VESTS TO this account?
  -- Sorted by amount descending (largest delegators first)
  ---------------------------------------------------------------------------
  _incoming_delegations := array_agg(row ORDER BY row.amount::BIGINT DESC)
  FROM (
    SELECT
      ba.delegator,
      ba.amount,
      ba.operation_id,
      ba.block_num
    FROM btracker_backend.incoming_delegations(_account_id) ba
  ) row;

  ---------------------------------------------------------------------------
  -- OUTGOING DELEGATIONS
  -- Query: Who has this account delegated VESTS TO?
  -- Sorted by amount descending (largest delegations first)
  ---------------------------------------------------------------------------
  _outgoing_delegations := array_agg(row ORDER BY row.amount::BIGINT DESC)
  FROM (
    SELECT
      ba.delegatee,
      ba.amount,
      ba.operation_id,
      ba.block_num
    FROM btracker_backend.outgoing_delegations(_account_id) ba
  ) row;

  ---------------------------------------------------------------------------
  -- RETURN COMPOSITE TYPE
  -- COALESCE ensures empty arrays instead of NULL for consistent JSON
  ---------------------------------------------------------------------------
  RETURN (
    COALESCE(_outgoing_delegations, '{}'::btracker_backend.outgoing_delegations[]),
    COALESCE(_incoming_delegations, '{}'::btracker_backend.incoming_delegations[])
  )::btracker_backend.delegations;
END
$$;

RESET ROLE;
