SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_transfers CASCADE;
CREATE TYPE btracker_backend.impacted_transfers AS
(
    nai INT,
    transfer_amount BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.get_impacted_transfers(IN _operation_body JSONB, IN _op_type_id INT)
RETURNS SETOF btracker_backend.impacted_transfers
LANGUAGE plpgsql
STABLE
AS
$BODY$
BEGIN
  IF _op_type_id = 2 OR _op_type_id = 83 THEN
    RETURN QUERY SELECT * FROM btracker_backend.process_transfer(_operation_body);
  ELSIF _op_type_id = 27 THEN
    RETURN QUERY SELECT * FROM btracker_backend.process_escrow_transfer(_operation_body);
  END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION btracker_backend.process_transfer(IN _operation_body JSONB)
RETURNS SETOF btracker_backend.impacted_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT 
      substring((_operation_body) -> 'value' -> 'amount' ->> 'nai', '[0-9]+')::INT AS nai,
      (_operation_body -> 'value' -> 'amount' ->> 'amount')::BIGINT AS transfer_amount
  );
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_escrow_transfer(IN _operation_body JSONB)
RETURNS SETOF btracker_backend.impacted_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY (
    SELECT
      substring((_operation_body) -> 'value' -> 'hive_amount' ->> 'nai', '[0-9]+')::INT AS nai,
      ((_operation_body) -> 'value' -> 'hive_amount' ->> 'amount')::BIGINT AS transfer_amount

    UNION ALL

    SELECT
      substring((_operation_body) -> 'value' -> 'hbd_amount' ->> 'nai', '[0-9]+')::INT AS nai,
      ((_operation_body) -> 'value' -> 'hbd_amount' ->> 'amount')::BIGINT AS transfer_amount
  );
  
END
$$;

RESET ROLE;
