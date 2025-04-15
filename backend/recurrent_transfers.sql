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
BEGIN
  WITH json_data AS (
    SELECT (_operation_body)->'value'->'extensions' AS extension_data
  )
  SELECT (inner_elem->>'pair_id')::INT INTO _pair_id
  FROM json_data,
  LATERAL jsonb_array_elements(extension_data) AS outer_elem,
  LATERAL jsonb_array_elements(outer_elem) AS inner_elem
  WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id';

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
BEGIN
  WITH json_data AS (
    SELECT (_operation_body)->'value'->'extensions' AS extension_data
  )
  SELECT inner_elem->>'pair_id' INTO _pair_id
  FROM json_data,
  LATERAL jsonb_array_elements(extension_data) AS outer_elem,
  LATERAL jsonb_array_elements(outer_elem) AS inner_elem
  WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id';

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
BEGIN
  WITH json_data AS (
    SELECT (_operation_body)->'value'->'extensions' AS extension_data
  )
  SELECT inner_elem->>'pair_id' INTO _pair_id
  FROM json_data,
  LATERAL jsonb_array_elements(extension_data) AS outer_elem,
  LATERAL jsonb_array_elements(outer_elem) AS inner_elem
  WHERE jsonb_typeof(inner_elem) = 'object' AND inner_elem ? 'pair_id';

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
    ((_operation_body)->'value'->>'deleted')::BOOLEAN
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
  __nai TEXT := '@@0000000' || nai;
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
