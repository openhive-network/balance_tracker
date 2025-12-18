/*
Operation parsers for recurring transfer operations.
Called by: db/process_recurrent_transfers.sql

Extracts data from operation JSONBs for:
- recurrent_transfer: Create/update/cancel scheduled transfers
- fill_recurrent_transfer: Successful execution of scheduled transfer
- failed_recurrent_transfer: Failed execution (insufficient funds)

Return type includes from/to accounts, transfer_id (pair_id from extensions),
amount, consecutive_failures, remaining_executions, recurrence, and delete flag.

Lifecycle:
- User creates transfer with amount, recurrence (hours), and execution count
- System fills periodically, decrementing remaining_executions
- On fill failure, consecutive_failures increments
- Transfer deleted when: amount=0, remaining_executions=0, or 10 consecutive failures
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.recurrent_transfer_return CASCADE;
CREATE TYPE btracker_backend.recurrent_transfer_return AS
(
    from_account         TEXT,
    to_account           TEXT,
    transfer_id          INT,
    nai                  INT,
    amount               BIGINT,
    consecutive_failures INT,
    remaining_executions INT,
    recurrence           INT,
    memo                 TEXT,
    delete_transfer      BOOLEAN
);

-- Extract pair_id from operation extensions.
-- pair_id uniquely identifies a transfer between two accounts (allows multiple).
-- Returns 0 if no pair_id specified (legacy transfers before pair_id feature).
CREATE OR REPLACE FUNCTION btracker_backend.extract_pair_id(IN __extensions JSONB)
RETURNS INT
LANGUAGE plpgsql
IMMUTABLE
AS
$BODY$
DECLARE
  __pair_id INT;
BEGIN
  __pair_id := (
    SELECT (outer_elem->'value'->>'pair_id')::INT
    FROM jsonb_array_elements(__extensions) AS outer_elem
    LIMIT 1
  );

  RETURN COALESCE(__pair_id, 0);
END;
$BODY$;

/*
{
  "type": "recurrent_transfer_operation",
  "value": {
    "to": "hive.helps",
    "from": "condeas",
    "memo": "Einzahlung Condeas",
    "amount": {
      "nai": "@@000000021",
      "amount": "25000",
      "precision": 3
    },
    "executions": 51,
    "extensions": [],
    "recurrence": 168
  }
}
*/

-- Extract recurrent_transfer data.
-- Creates/updates/cancels a scheduled recurring transfer.
-- Amount of 0 cancels the transfer (marks for deletion).
CREATE OR REPLACE FUNCTION btracker_backend.process_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __amount       btracker_backend.asset := btracker_backend.parse_amount_object(__operation_body -> 'value' -> 'amount');
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __del_transfer BOOLEAN := FALSE;  -- mark transfer for deletion if amount is 0
  __con_failures INT     := 0;      -- new/updated transfer starts with 0 failures

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- Amount of 0 cancels the recurring transfer
  IF __amount.amount = 0 THEN
    __del_transfer := TRUE;
  END IF;

  __return := (
    __operation_body -> 'value' ->> 'from',       -- from_account
    __operation_body -> 'value' ->> 'to',         -- to_account
    __pair_id,                                    -- transfer_id
    __amount.asset_symbol_nai,                    -- nai
    __amount.amount,                              -- amount
    __con_failures,                               -- consecutive_failures
    __operation_body -> 'value' ->> 'executions', -- remaining_executions
    __operation_body -> 'value' ->> 'recurrence', -- recurrence
    __operation_body -> 'value' ->> 'memo',       -- memo
    __del_transfer                                -- delete_transfer
  );

  RETURN __return;
END
$$;

/*
{
  "type": "fill_recurrent_transfer_operation",
  "value": {
    "to": "risingstar2",
    "from": "hannes-stoffel",
    "memo": "Ich sitze hier auf einem Kutter, an nem Tisch und Esse Fisch, leider ohne Butter.",
    "amount": {
      "nai": "@@000000021",
      "amount": "1",
      "precision": 3
    },
    "remaining_executions": 124
  }
}
*/

-- Extract fill_recurrent_transfer data (virtual op).
-- Successful execution of scheduled transfer. Decrements remaining_executions.
-- Resets consecutive_failures to 0. Deletes transfer when remaining_executions = 0.
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __rem_exec     INT     := (__operation_body->'value'->>'remaining_executions')::INT;
  __del_transfer BOOLEAN := FALSE;  -- mark for deletion when all executions complete
  __con_failures INT     := 0;      -- successful fill resets failure counter

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- All executions complete, delete the transfer
  IF __rem_exec = 0 THEN
    __del_transfer := TRUE;
  END IF;

  __return := (
    __operation_body -> 'value' ->> 'from', -- from_account
    __operation_body -> 'value' ->> 'to',   -- to_account
    __pair_id,                              -- transfer_id
    NULL,                                   -- nai         not included in fill operation
    NULL,                                   -- amount      not included in fill operation
    __con_failures,                         -- consecutive_failures
    __rem_exec,                             -- remaining_executions
    NULL,                                   -- recurrence  not included in fill operation
    NULL,                                   -- memo        not included in fill operation
    __del_transfer                          -- delete_transfer
  );

  RETURN __return;
END
$$;

/*

{
  "type": "failed_recurrent_transfer_operation",
  "value": {
    "to": "eds-holdings",
    "from": "siddhartaz",
    "memo": "SPI subscription 2 - 50 SPI",
    "amount": {
      "nai": "@@000000021",
      "amount": "7500",
      "precision": 3
    },
    "deleted": false,
    "consecutive_failures": 8,
    "remaining_executions": 11
  }
}

{
  "type": "failed_recurrent_transfer_operation",
  "value": {
    "to": "risingstar2",
    "from": "mateodm03",
    "memo": "",
    "amount": {
      "nai": "@@000000021",
      "amount": "1",
      "precision": 3
    },
    "deleted": true,
    "consecutive_failures": 10,
    "remaining_executions": 11
  }
}

*/

-- Extract failed_recurrent_transfer data (virtual op).
-- Failed execution (usually insufficient funds). Increments consecutive_failures.
-- Transfer deleted after 10 consecutive failures OR if explicitly deleted.
CREATE OR REPLACE FUNCTION btracker_backend.process_fail_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __rem_exec     INT     := (__operation_body->'value'->>'remaining_executions')::INT;
  __deleted      BOOLEAN := (__operation_body->'value'->>'deleted')::BOOLEAN;  -- true after 10 failures
  __del_transfer BOOLEAN := FALSE;  -- mark for deletion on 10 failures or explicit delete
  __con_failures INT     := (__operation_body->'value'->>'consecutive_failures')::INT;

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- Delete if no executions remain or max failures reached (deleted=true)
  IF __rem_exec = 0 OR __deleted THEN
    __del_transfer := TRUE;
  END IF;

  __return := (
    __operation_body -> 'value' ->> 'from',       -- from_account
    __operation_body -> 'value' ->> 'to',         -- to_account
    __pair_id,                                    -- transfer_id
    NULL,                                         -- nai         not included in fail operation
    NULL,                                         -- amount      not included in fail operation
    __con_failures,                               -- consecutive_failures
    __rem_exec,                                   -- remaining_executions
    NULL,                                         -- recurrence  not included in fail operation
    NULL,                                         -- memo        not included in fail operation
    __del_transfer                                -- delete_transfer
  );

  RETURN __return;
END
$$;

RESET ROLE;
