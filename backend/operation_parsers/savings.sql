/*
Operation parsers for savings operations.
Called by: db/process_savings.sql

Extracts data from operation JSONBs for:
- transfer_to_savings: Deposit into savings (increases balance)
- transfer_from_savings: Initiate 3-day withdrawal (decreases balance, creates request)
- fill_transfer_from_savings: Complete withdrawal after 3-day delay
- cancel_transfer_from_savings: Cancel pending withdrawal request
- interest: HBD savings interest payment

Return type includes account, amount, asset NAI, and savings_withdraw_request flag:
  0 = balance change only (deposit/interest)
  1 = new withdrawal request created
 -1 = withdrawal request completed or cancelled
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_savings_return CASCADE;
CREATE TYPE btracker_backend.impacted_savings_return AS
(
    account_name VARCHAR,
    amount BIGINT,
    asset_symbol_nai INT,
    savings_withdraw_request INT,
    request_id BIGINT
);

-- Extract transfer_to_savings data.
-- Deposit to savings account (balance increase, no pending request).
CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_to_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  __amount btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'amount');
BEGIN
  RETURN (
    (_operation_body -> 'value' ->> 'to')::TEXT,  -- recipient of savings deposit
    __amount.amount,                               -- positive: increases balance
    __amount.asset_symbol_nai,
    0,                                             -- 0 = no pending request change
    NULL
  )::btracker_backend.impacted_savings_return;

END
$$;

-- Extract transfer_from_savings data.
-- Initiate withdrawal (3-day delay). Creates pending request.
CREATE OR REPLACE FUNCTION btracker_backend.process_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  __amount btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'amount');
BEGIN
  RETURN (
    (_operation_body -> 'value' ->> 'from')::TEXT,  -- account withdrawing
    - __amount.amount,                               -- negative: decreases savings balance
    __amount.asset_symbol_nai,
    1,                                               -- 1 = new pending request created
    (_operation_body -> 'value' ->> 'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

-- Extract fill_transfer_from_savings data (virtual op).
-- Completes withdrawal after 3-day delay. Clears pending request.
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  __amount btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'amount');
BEGIN
  RETURN (
    (_operation_body -> 'value' ->> 'from')::TEXT,  -- original requester
    0,                                               -- no balance change (already deducted)
    __amount.asset_symbol_nai,
    -1,                                              -- -1 = pending request completed
    (_operation_body -> 'value' ->> 'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

-- Extract interest_operation data (virtual op).
-- HBD savings interest payment (monthly, ~20% APR variable).
CREATE OR REPLACE FUNCTION btracker_backend.process_interest_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  __amount btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'interest');
BEGIN
  RETURN (
    (_operation_body -> 'value' ->> 'owner')::TEXT,  -- account receiving interest
    __amount.amount,                                  -- positive: increases savings balance
    __amount.asset_symbol_nai,
    0,                                                -- no pending request change
    NULL
  )::btracker_backend.impacted_savings_return;

END
$$;

-- Extract cancel_transfer_from_savings data.
-- Cancels pending withdrawal request (funds returned to savings).
CREATE OR REPLACE FUNCTION btracker_backend.process_cancel_transfer_from_savings_operation(IN _operation_body JSONB)
RETURNS btracker_backend.impacted_savings_return
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,  -- account cancelling
    NULL,                                          -- balance restored separately
    NULL,
    -1,                                            -- -1 = pending request removed
    ((_operation_body)->'value'->>'request_id')::BIGINT
  )::btracker_backend.impacted_savings_return;

END
$$;

RESET ROLE;
