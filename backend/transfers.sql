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
DECLARE
  __amount btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'amount');
BEGIN
  RETURN QUERY (
    SELECT 
      __amount.asset_symbol_nai AS nai,
      __amount.amount AS transfer_amount
  );
  
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.process_escrow_transfer(IN _operation_body JSONB)
RETURNS SETOF btracker_backend.impacted_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
DECLARE
  __hive_amt btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'hive_amount');
  __hbd_amt  btracker_backend.asset := btracker_backend.parse_amount_object(_operation_body -> 'value' -> 'hbd_amount');
BEGIN
  RETURN QUERY (
    SELECT
      __hive_amt.asset_symbol_nai AS nai,
      __hive_amt.amount AS transfer_amount

    UNION ALL

    SELECT
      __hbd_amt.asset_symbol_nai AS nai,
      __hbd_amt.amount AS transfer_amount
  );
  
END
$$;

RESET ROLE;
