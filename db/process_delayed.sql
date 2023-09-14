CREATE OR REPLACE FUNCTION btracker_app.process_transfer_to_vesting_completed_operation(body jsonb)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH transfer_to_vesting_completed_operation AS (
    SELECT 
        (SELECT id FROM hive.btracker_app_accounts_view WHERE name = body->'value'->>'to_account') AS _account,
        (body->'value'->'vesting_shares_received'->>'amount')::BIGINT AS _delayed_vests
)
  INSERT INTO btracker_app.account_withdraws 
  (
    account,
    delayed_vests
    )
    SELECT 
      _account,
      _delayed_vests
    FROM transfer_to_vesting_completed_operation

    ON CONFLICT ON CONSTRAINT pk_account_withdraws
    DO UPDATE SET
        delayed_vests = btracker_app.account_withdraws.delayed_vests + EXCLUDED.delayed_vests;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_delayed_voting_operation(body jsonb)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH delayed_voting_operation AS (
    SELECT 
        (SELECT id FROM hive.btracker_app_accounts_view WHERE name = body->'value'->>'voter') AS _account,
        (body->'value'->>'votes')::BIGINT AS _delayed_vests
)
  INSERT INTO btracker_app.account_withdraws 
  (
    account,
    delayed_vests
    )
    SELECT 
      _account,
      _delayed_vests
    FROM delayed_voting_operation

    ON CONFLICT ON CONSTRAINT pk_account_withdraws
    DO UPDATE SET
        delayed_vests = GREATEST(btracker_app.account_withdraws.delayed_vests - EXCLUDED.delayed_vests, 0);
END
$$
;
