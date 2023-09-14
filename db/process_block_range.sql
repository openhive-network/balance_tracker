CREATE OR REPLACE FUNCTION btracker_app.process_block_range_data_c(in _from INT, in _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
SET cursor_tuple_fraction='0.9'
AS
$$
DECLARE
  __balance_change RECORD;
  ___balance_change RECORD;
  __current_balance BIGINT;
  __last_reported_block INT := 0;
  ___last_reported_block INT := 0;
  _vesting_multiplication RECORD;
  _to_withdraw BIGINT;
  _withdraw_rate INT := (SELECT withdraw_rate FROM btracker_app.app_status);
  _start_delayed_vests BOOLEAN := (SELECT start_delayed_vests FROM btracker_app.app_status);
BEGIN
RAISE NOTICE 'Processing balances';
FOR __balance_change IN
  WITH balance_impacting_ops AS
  (
    SELECT ot.id
    FROM hive.operation_types ot
    WHERE ot.name IN (SELECT * FROM hive.get_balance_impacting_operations())
  )
  SELECT av.id AS account, bio.asset_symbol_nai AS nai, bio.amount as balance, ho.id AS source_op, ho.block_num AS source_op_block
  FROM hive.btracker_app_operations_view ho --- APP specific view must be used, to correctly handle reversible part of the data.
  JOIN balance_impacting_ops b ON ho.op_type_id = b.id
  JOIN LATERAL
  (
    /*
      There was in the block 905693 a HF1 that generated bunch of virtual operations `vesting_shares_split_operation`( above 5000 ).
      This operation multiplied VESTS by milion for every account.
    */
    SELECT * FROM hive.get_impacted_balances(ho.body_binary, ho.block_num > 905693)
  ) bio ON true
  JOIN hive.accounts_view av ON av.name = bio.account_name
  WHERE ho.block_num BETWEEN _from AND _to
  ORDER BY ho.block_num, ho.id
LOOP
  INSERT INTO btracker_app.current_account_balances
    (account, nai, source_op, source_op_block, balance)
  SELECT __balance_change.account, __balance_change.nai, __balance_change.source_op, __balance_change.source_op_block, __balance_change.balance
  ON CONFLICT ON CONSTRAINT pk_current_account_balances DO
    UPDATE SET balance = btracker_app.current_account_balances.balance + EXCLUDED.balance,
               source_op = EXCLUDED.source_op,
               source_op_block = EXCLUDED.source_op_block
  RETURNING balance into __current_balance;

  INSERT INTO btracker_app.account_balance_history
    (account, nai, source_op, source_op_block, balance)
  SELECT __balance_change.account, __balance_change.nai, __balance_change.source_op, __balance_change.source_op_block, COALESCE(__current_balance, 0)
  ;

--	if __balance_change.source_op_block = 199701 then
--	RAISE NOTICE 'Balance change data: account: %, amount change: %, source_op: %, source_block: %, current_balance: %', __balance_change.account, __balance_change.balance, __balance_change.source_op, __balance_change.source_op_block, COALESCE(__current_balance, 0);
--	end if;

  IF __balance_change.source_op_block % _report_step = 0 AND __last_reported_block != __balance_change.source_op_block THEN
    RAISE NOTICE 'Processed data for block: %', __balance_change.source_op_block;
    __last_reported_block := __balance_change.source_op_block;
  END IF;
END LOOP;

RAISE NOTICE 'Processing delegations, rewards, savings, withdraws';

FOR ___balance_change IN
  SELECT 
    ov.body AS body,
    ov.id AS source_op,
    ov.block_num as source_op_block,
    ov.op_type_id as op_type
  FROM hive.btracker_app_operations_view ov
  WHERE 
    (ov.op_type_id IN (40,41,62,32,33,34,59,39,4,20,56,60,52,53,77,70,68) or
    (ov.op_type_id = 55 and (ov.body->'value'->>'is_saved_into_hbd_balance')::BOOLEAN = false)  or
    (ov.op_type_id IN (51,63) and (ov.body->'value'->>'payout_must_be_claimed')::BOOLEAN = true))
    AND ov.block_num BETWEEN _from AND _to
  ORDER BY ov.block_num, ov.id

--delegations (40,41,62)
--savings (32,33,34,59,55)
--rewards (39,51,52,63,53)
--withdraws (4,20,56)
--hardforks (60)

LOOP

  CASE 

    WHEN ___balance_change.op_type = 40 THEN
    PERFORM btracker_app.process_delegate_vesting_shares_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 41 THEN
    PERFORM btracker_app.process_account_create_with_delegation_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 62 THEN
    PERFORM btracker_app.process_return_vesting_delegation_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 32 THEN
    PERFORM btracker_app.process_transfer_to_savings_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 33 THEN
    PERFORM btracker_app.process_transfer_from_savings_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 34 THEN
    PERFORM btracker_app.process_cancel_transfer_from_savings_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 59 THEN
    PERFORM btracker_app.process_fill_transfer_from_savings_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 55 THEN
    PERFORM btracker_app.process_interest_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 39 THEN
    PERFORM btracker_app.process_claim_reward_balance_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 51 THEN
    PERFORM btracker_app.process_author_reward_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 52 THEN
    PERFORM btracker_app.process_curation_reward_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 63 THEN
    PERFORM btracker_app.process_comment_benefactor_reward_operation(___balance_change.body, ___balance_change.source_op, ___balance_change.source_op_block);

    WHEN ___balance_change.op_type = 4 THEN
    PERFORM btracker_app.process_withdraw_vesting_operation(___balance_change.body, _withdraw_rate);

    WHEN ___balance_change.op_type = 20 THEN
    PERFORM btracker_app.process_set_withdraw_vesting_route_operation(___balance_change.body);

    WHEN ___balance_change.op_type = 56 THEN
    PERFORM btracker_app.process_fill_vesting_withdraw_operation(___balance_change.body, _start_delayed_vests);

    WHEN ___balance_change.op_type = 53 THEN
    PERFORM btracker_app.process_comment_reward_operation(___balance_change.body);

    WHEN ___balance_change.op_type = 77 AND _start_delayed_vests = TRUE THEN
    PERFORM btracker_app.process_transfer_to_vesting_completed_operation(___balance_change.body);

    WHEN ___balance_change.op_type = 70 AND _start_delayed_vests = TRUE THEN
    PERFORM btracker_app.process_delayed_voting_operation(___balance_change.body);

    WHEN ___balance_change.op_type = 68 THEN
    PERFORM btracker_app.process_hardfork_hive_operation(___balance_change.body);

    WHEN ___balance_change.op_type = 60 THEN
    
      CASE ((___balance_change.body)->'value'->>'hardfork_id')::INT
      -- HARDFORK 1
      WHEN 1 THEN
        FOR _vesting_multiplication IN
        SELECT 
          account, 
          vesting_withdraw_rate, 
          to_withdraw, 
          withdrawn 
        FROM btracker_app.account_withdraws

        LOOP

        SELECT _vesting_multiplication.to_withdraw * 1000000 INTO _to_withdraw;

        UPDATE btracker_app.account_withdraws SET 
          vesting_withdraw_rate = _to_withdraw / _withdraw_rate,
          to_withdraw = _to_withdraw,
          withdrawn = _vesting_multiplication.withdrawn * 1000000
        WHERE account = _vesting_multiplication.account;

        END LOOP;
      -- HARDFORK 16
      WHEN 16 THEN
        UPDATE btracker_app.app_status SET withdraw_rate = 13;
        _withdraw_rate = 13;

      WHEN 24 THEN
        UPDATE btracker_app.app_status SET start_delayed_vests = TRUE;
        _start_delayed_vests = TRUE;
    
      ELSE
      END CASE;

    ELSE
  END CASE;

END LOOP;
END
$$
;


CREATE OR REPLACE FUNCTION btracker_app.process_hardfork_hive_operation(body jsonb)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
BEGIN
WITH hardfork_hive_operation AS MATERIALIZED
(
  SELECT 
    (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'account') AS _account
),
balances AS (
  UPDATE btracker_app.current_account_balances SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
rewards AS (
  UPDATE btracker_app.account_rewards SET
    balance = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
delegations AS (
  UPDATE btracker_app.account_delegations SET
    delegated_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
),
savings AS (
  UPDATE btracker_app.account_savings SET
    saving_balance = 0,
    savings_withdraw_requests = 0
  FROM hardfork_hive_operation
  WHERE account = _account
)
  UPDATE btracker_app.account_withdraws SET
    vesting_withdraw_rate = 0,
    to_withdraw = 0,
    withdrawn = 0,
    delayed_vests = 0
  FROM hardfork_hive_operation
  WHERE account = _account;

END
$$
;
