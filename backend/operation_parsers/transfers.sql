/*
Operation parsers for transfer statistics.
Called by: db/process_transfer_stats.sql

Extracts transfer amounts from operation JSONBs for:
- transfer: Standard HIVE/HBD transfer (single amount)
- fill_recurrent_transfer: Executed recurring transfer (single amount)
- escrow_transfer: Escrow creation (returns both HIVE and HBD amounts)

Return type is simple (nai, amount) for aggregation into hourly/daily/monthly stats.
escrow_transfer returns SETOF with two rows (one per asset type).
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.impacted_transfers CASCADE;
CREATE TYPE btracker_backend.impacted_transfers AS
(
    nai INT,
    transfer_amount BIGINT
);

-- Extract transfer amount for statistics.
-- Handles both regular transfer and fill_recurrent_transfer operations.
-- Returns single row with (nai, amount) for aggregation.
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

-- Extract escrow_transfer amounts for statistics.
-- Escrow can contain both HIVE and HBD, so returns two rows.
-- Both amounts are counted toward transfer volume statistics.
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
