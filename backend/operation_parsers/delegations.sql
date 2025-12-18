/*
Operation parsers for delegation operations.
Called by: db/process_delegations.sql

Extracts data from operation JSONBs for:
- delegate_vesting_shares: Delegation between accounts
- account_create_with_delegation: Initial delegation at account creation
- return_vesting_delegation: Undelegation after 5-day delay (virtual op)
- hardfork_hive: HF23 delegation reset for Steemit Inc accounts

Return type includes delegator, delegatee, and vesting share amount.
Negative amounts indicate vests returning to the delegator.
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_delegations_return CASCADE;
CREATE TYPE btracker_backend.impacted_delegations_return AS
(
    delegator VARCHAR,
    delegatee VARCHAR,
    amount BIGINT
);

-- Extract delegate_vesting_shares data.
-- Sets total delegation from delegator to delegatee (not a delta).
CREATE OR REPLACE FUNCTION btracker_backend.process_delegate_vesting_shares_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'delegator')::TEXT,
    ((_operation_body)->'value'->>'delegatee')::TEXT,
    ((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT  -- absolute amount, not delta
  )::btracker_backend.impacted_delegations_return;

END
$$;

-- Extract account_create_with_delegation data.
-- Initial delegation from creator to new account at creation time.
CREATE OR REPLACE FUNCTION btracker_backend.process_account_create_with_delegation_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'creator')::TEXT,       -- delegator (account creator)
    ((_operation_body)->'value'->>'new_account_name')::TEXT,  -- delegatee (new account)
    ((_operation_body)->'value'->'delegation'->>'amount')::BIGINT
  )::btracker_backend.impacted_delegations_return;

END
$$;

-- Extract return_vesting_delegation data (virtual op).
-- Vests return to delegator after 5-day delay when delegation is reduced/removed.
CREATE OR REPLACE FUNCTION btracker_backend.process_return_vesting_shares_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,  -- delegator receiving vests back
    NULL,                                             -- no delegatee (vests returning)
    - ((_operation_body)->'value'->'vesting_shares'->>'amount')::BIGINT  -- negative: reduces delegated amount
  )::btracker_backend.impacted_delegations_return;

END
$$;

-- Extract HF23 affected account for delegation reset.
-- Steemit Inc accounts had all delegations zeroed at the Hive fork.
CREATE OR REPLACE FUNCTION btracker_backend.process_hf23_acccounts(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_delegations_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'account')::TEXT,  -- affected account
    NULL,                                             -- all delegations cleared
    0                                                 -- zero out delegated vests
  )::btracker_backend.impacted_delegations_return;

END
$$;

RESET ROLE;
