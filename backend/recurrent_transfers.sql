SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.recurrent_transfer_return CASCADE;
CREATE TYPE btracker_backend.recurrent_transfer_return AS
(
    from_account TEXT,
    to_account TEXT,
    transfer_id INT,
    nai INT,
    amount BIGINT,
    consecutive_failures INT,
    remaining_executions INT,
    recurrence INT,
    memo TEXT,
    delete_transfer BOOLEAN
);

CREATE OR REPLACE FUNCTION btracker_backend.get_recurrent_transfer_operations(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  RETURN (
    CASE 
      WHEN _op_type_id = 49 THEN
        btracker_backend.process_recurrent_transfer_operations(_operation_body)

      WHEN _op_type_id = 83 THEN
        btracker_backend.process_fill_recurrent_transfer_operations(_operation_body)

      WHEN _op_type_id = 84 THEN
        btracker_backend.process_fail_recurrent_transfer_operations(_operation_body) 
    END
  );

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

{
  "id": 1,
  "jsonrpc": "2.0",
  "result": [
    {
      "amount": "0.001 HIVE",
      "consecutive_failures": 0,
      "from": "clove71",
      "id": 2205,
      "memo": "",
      "pair_id": 0,
      "recurrence": 24,
      "remaining_executions": 60,
      "to": "risingstar2",
      "trigger_date": "2025-04-15T15:09:51"
    },
    {
      "amount": "0.001 HIVE",
      "consecutive_failures": 0,
      "from": "clove71",
      "id": 2232,
      "memo": "",
      "pair_id": 0,
      "recurrence": 24,
      "remaining_executions": 87,
      "to": "risingstargame2",
      "trigger_date": "2025-04-15T11:56:39"
    }
  ]
}

*/

CREATE OR REPLACE FUNCTION btracker_backend.process_recurrent_transfer_operations(IN _operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE 
  _pair_id INT;
  _amount BIGINT := ((_operation_body)->'value'->'amount'->>'amount')::BIGINT;
  _extension_data JSONB;
  _first_elem JSONB;
BEGIN
  _extension_data := (_operation_body)->'value'->'extensions';
  
  -- Handle both pre-HF28 (nested arrays) and post-HF28 (direct objects) formats
  IF jsonb_array_length(_extension_data) > 0 THEN
    _first_elem := _extension_data->0;
    
    -- Check if we have the old nested array format or new direct object format
    IF jsonb_typeof(_first_elem) = 'array' THEN
      -- Old format: array of arrays
      SELECT (inner_elem->>'pair_id')::INT INTO _pair_id
      FROM jsonb_array_elements(_extension_data) AS outer_elem,
      LATERAL jsonb_array_elements(outer_elem) AS inner_elem
      WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id'
      LIMIT 1;
    ELSIF jsonb_typeof(_first_elem) = 'object' AND _first_elem ? 'pair_id' THEN
      -- New format: array of objects directly
      _pair_id := (_first_elem->>'pair_id')::INT;
    END IF;
  END IF;

  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    ((_operation_body)->'value'->>'to')::TEXT,
    COALESCE(_pair_id, 0),
    substring((_operation_body)->'value'->'amount'->>'nai', '[0-9]+')::INT,
    _amount,
    0,
    ((_operation_body)->'value'->>'executions')::INT,
    ((_operation_body)->'value'->>'recurrence')::INT,
    ((_operation_body)->'value'->>'memo')::TEXT,
    (CASE WHEN _amount = 0 THEN TRUE ELSE FALSE END)
  )::btracker_backend.recurrent_transfer_return;
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

CREATE OR REPLACE FUNCTION btracker_backend.process_fill_recurrent_transfer_operations(IN _operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE 
  _pair_id INT;
  _extension_data JSONB;
  _first_elem JSONB;
BEGIN
  _extension_data := (_operation_body)->'value'->'extensions';
  
  -- Handle both pre-HF28 (nested arrays) and post-HF28 (direct objects) formats
  IF jsonb_array_length(_extension_data) > 0 THEN
    _first_elem := _extension_data->0;
    
    -- Check if we have the old nested array format or new direct object format
    IF jsonb_typeof(_first_elem) = 'array' THEN
      -- Old format: array of arrays
      SELECT inner_elem->>'pair_id' INTO _pair_id
      FROM jsonb_array_elements(_extension_data) AS outer_elem,
      LATERAL jsonb_array_elements(outer_elem) AS inner_elem
      WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id'
      LIMIT 1;
    ELSIF jsonb_typeof(_first_elem) = 'object' AND _first_elem ? 'pair_id' THEN
      -- New format: array of objects directly
      _pair_id := (_first_elem->>'pair_id')::INT;
    END IF;
  END IF;

  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    ((_operation_body)->'value'->>'to')::TEXT,
    COALESCE(_pair_id, 0),
    NULL,
    NULL,
    0,
    ((_operation_body)->'value'->>'remaining_executions')::INT,
    NULL,
    NULL,
    (CASE WHEN ((_operation_body)->'value'->>'remaining_executions')::INT = 0 THEN TRUE ELSE FALSE END)
  )::btracker_backend.recurrent_transfer_return;
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

CREATE OR REPLACE FUNCTION btracker_backend.process_fail_recurrent_transfer_operations(IN _operation_body JSONB)
RETURNS btracker_backend.recurrent_transfer_return
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE 
  _pair_id INT;
  _delete BOOLEAN;
  _extension_data JSONB;
  _first_elem JSONB;
BEGIN
  _extension_data := (_operation_body)->'value'->'extensions';
  
  -- Handle both pre-HF28 (nested arrays) and post-HF28 (direct objects) formats
  IF jsonb_array_length(_extension_data) > 0 THEN
    _first_elem := _extension_data->0;
    
    -- Check if we have the old nested array format or new direct object format
    IF jsonb_typeof(_first_elem) = 'array' THEN
      -- Old format: array of arrays
      SELECT inner_elem->>'pair_id' INTO _pair_id
      FROM jsonb_array_elements(_extension_data) AS outer_elem,
      LATERAL jsonb_array_elements(outer_elem) AS inner_elem
      WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id'
      LIMIT 1;
    ELSIF jsonb_typeof(_first_elem) = 'object' AND _first_elem ? 'pair_id' THEN
      -- New format: array of objects directly
      _pair_id := (_first_elem->>'pair_id')::INT;
    END IF;
  END IF;

  -- FIX for a bug in failed_recurrent_transfer_operation - delete is not set when remaining_executions = 0
  _delete := (
    CASE
      WHEN ((_operation_body)->'value'->>'deleted')::BOOLEAN = TRUE OR ((_operation_body)->'value'->>'remaining_executions')::INT = 0 THEN
        TRUE
      ELSE
        FALSE
    END
  );

  RETURN (
    ((_operation_body)->'value'->>'from')::TEXT,
    ((_operation_body)->'value'->>'to')::TEXT,
    COALESCE(_pair_id, 0),
    NULL,
    NULL,
    ((_operation_body)->'value'->>'consecutive_failures')::INT,
    ((_operation_body)->'value'->>'remaining_executions')::INT,
    NULL,
    NULL,
    _delete
  )::btracker_backend.recurrent_transfer_return;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.amount_object(IN _nai INT, IN _amount BIGINT)
RETURNS btracker_backend.amount
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE 
  __precision INT;
  __nai TEXT := '@@0000000' || _nai;
BEGIN
  IF _nai = 13 THEN
    __precision := 3;
  ELSIF _nai = 21 THEN
    __precision := 3;
  ELSIF _nai = 37 THEN
    __precision := 6;
  END IF;

  RETURN (
    __nai,
    _amount::TEXT,
    __precision
  )::btracker_backend.amount;
END
$$;



RESET ROLE;
