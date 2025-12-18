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

-- Helper function to extract pair_id from extensions
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

-- Process recurrent_transfer_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __amount       btracker_backend.asset := btracker_backend.parse_amount_object(__operation_body -> 'value' -> 'amount');
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __del_transfer BOOLEAN := FALSE; -- mark transfer for deletion if amount is 0
  __con_failures INT     := 0;     -- first transfer, always 0

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- if amount is 0, mark transfer for deletion
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

-- Process fill_recurrent_transfer_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_fill_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __rem_exec     INT     := (__operation_body->'value'->>'remaining_executions')::INT;
  __del_transfer BOOLEAN := FALSE; -- mark transfer for deletion if remaining executions is 0
  __con_failures INT     := 0;     -- resets consecutive failures on fill is always 0

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- if remaining executions is 0, mark transfer for deletion
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

-- Process failed_recurrent_transfer_operation
CREATE OR REPLACE FUNCTION btracker_backend.process_fail_recurrent_transfer_operations(IN __operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __pair_id      INT     := btracker_backend.extract_pair_id((__operation_body)->'value'->'extensions');
  __rem_exec     INT     := (__operation_body->'value'->>'remaining_executions')::INT;
  __deleted      BOOLEAN := (__operation_body->'value'->>'deleted')::BOOLEAN;
  __del_transfer BOOLEAN := FALSE; -- mark transfer for deletion if remaining executions is 0 or deleted is true
  __con_failures INT     := (__operation_body->'value'->>'consecutive_failures')::INT;

  __return btracker_backend.recurrent_transfer_return;
BEGIN
  -- if remaining executions is 0, mark transfer for deletion
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
