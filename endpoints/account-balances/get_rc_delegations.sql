SET ROLE btracker_owner;

/** openapi:paths
/accounts/{account-name}/rc-delegations:
  get:
    tags:
      - Accounts
    summary: Account RC delegations
    description: |
      List of incoming and outgoing RC (Resource Credit) delegations

      SQL example
      * `SELECT * FROM btracker_endpoints.get_rc_delegations(''blocktrades'');`

      REST call example
      * `GET ''https://%1$s/balance-api/accounts/blocktrades/rc-delegations''`
    operationId: btracker_endpoints.get_rc_delegations
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
          Incoming and outgoing RC delegations

          * Returns `btracker_backend.rc_delegations`
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/btracker_backend.rc_delegations'
            example: {
                  "outgoing_delegations": [],
                  "incoming_delegations": []
                }
      '404':
        description: No such account in the database
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_rc_delegations;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_rc_delegations(
    "account-name" TEXT
)
RETURNS btracker_backend.rc_delegations 
-- openapi-generated-code-end
LANGUAGE 'plpgsql' STABLE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
/*
================================================================================
ENDPOINT: get_rc_delegations
================================================================================
PURPOSE:
  Returns all active RC (Resource Credit) delegations for an account, split into
  two arrays:
  - Incoming delegations (RC received from other accounts)
  - Outgoing delegations (RC delegated to other accounts)

ARCHITECTURE:
  1. Resolves account name to account_id (404 on missing)
  2. Queries current_rc_delegations table via backend helpers
  3. Aggregates results into typed arrays, sorted by max_rc descending

DATA FLOW:
  current_rc_delegations -> incoming_rc_delegations() / outgoing_rc_delegations()
                         -> array_agg with ORDER BY max_rc DESC
                         -> btracker_backend.rc_delegations composite

RC DELEGATION MECHANICS:
  - RC delegations transfer Resource Credits without transferring ownership
  - Delegator retains the underlying VESTS but shares RC capacity
  - Delegatee gains additional RC for transactions
  - No return delay when removing RC delegations (unlike VEST delegations)
  - RC delegations were introduced in HF26

PLANNER HINTS:
  - from_collapse_limit = 16: Allow planner to consider more join orderings
  - join_collapse_limit = 16: Higher threshold for explicit JOIN optimization
  - jit = OFF: Disable JIT compilation (faster for simple queries)

PERFORMANCE NOTES:
  - Backend helpers perform account name lookups via hive.accounts_view
  - Results sorted by max_rc DESC to show largest delegations first
  - Empty arrays returned as '{}' (not NULL) for consistent JSON output

RETURN TYPE: btracker_backend.rc_delegations
  - outgoing_delegations[]: Array of (delegatee, max_rc, operation_id, block_num)
  - incoming_delegations[]: Array of (delegator, max_rc, operation_id, block_num)
================================================================================
*/
DECLARE
  _account_id INT := btracker_backend.get_account_id("account-name", TRUE);
  _incoming_delegations btracker_backend.incoming_rc_delegations[];
  _outgoing_delegations btracker_backend.outgoing_rc_delegations[];
BEGIN
  -- Set short cache - delegations can change frequently
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  ---------------------------------------------------------------------------
  -- INCOMING RC DELEGATIONS
  -- Query: Who has delegated RC TO this account?
  -- Sorted by max_rc descending (largest delegators first)
  ---------------------------------------------------------------------------
  _incoming_delegations := array_agg(row ORDER BY row.max_rc::BIGINT DESC)
  FROM (
    SELECT
      ba.delegator,
      ba.max_rc,
      ba.operation_id,
      ba.block_num
    FROM btracker_backend.incoming_rc_delegations(_account_id) ba
  ) row;

  ---------------------------------------------------------------------------
  -- OUTGOING RC DELEGATIONS
  -- Query: Who has this account delegated RC TO?
  -- Sorted by max_rc descending (largest delegations first)
  ---------------------------------------------------------------------------
  _outgoing_delegations := array_agg(row ORDER BY row.max_rc::BIGINT DESC)
  FROM (
    SELECT
      ba.delegatee,
      ba.max_rc,
      ba.operation_id,
      ba.block_num
    FROM btracker_backend.outgoing_rc_delegations(_account_id) ba
  ) row;

  ---------------------------------------------------------------------------
  -- RETURN COMPOSITE TYPE
  -- COALESCE ensures empty arrays instead of NULL for consistent JSON
  ---------------------------------------------------------------------------
  RETURN (
    COALESCE(_outgoing_delegations, '{}'::btracker_backend.outgoing_rc_delegations[]),
    COALESCE(_incoming_delegations, '{}'::btracker_backend.incoming_rc_delegations[])
  )::btracker_backend.rc_delegations;
END
$$;

RESET ROLE;
