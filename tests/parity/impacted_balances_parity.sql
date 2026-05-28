/*
 * impacted_balances_parity.sql
 * ============================
 * Drift guard for the SQL/JSONB reimplementation of the impacted-balances visitor
 * (issue #53). For each fixture operation it runs BOTH implementations:
 *
 *   - hive.get_impacted_balances(<op>::hafd.operation, is_hf01)        -- C++ visitor
 *   - btracker_backend.get_impacted_balances(type, value, is_hf01)     -- SQL/JSONB port
 *
 * and asserts the two row sets are equal as MULTISETS (duplicate rows must match in
 * count too -- e.g. escrow_rejected emits two identical HIVE rows). Any divergence
 * raises an exception, so run under `psql -v ON_ERROR_STOP=on`.
 *
 * Fixtures cover every operation in hive.get_balance_impacting_operations(): each
 * emitting overload (most bodies lifted verbatim from the HAF functional fixtures
 * under haf/tests/.../shared_lib/get_impacted_balances/), both is_hf01 settings for
 * VESTS-bearing ops, payout_must_be_claimed true/false, interest saved/not-saved, and
 * escrow fees in HIVE vs HBD. The five no-op overloads (clear_null_account_balance,
 * escrow_approve, transfer_to_vesting, transfer_from_savings, limit_order_cancel) are
 * confirmed empty in forward_impacted.cpp; they are checked SQL-side for "emits
 * nothing" rather than fed through the strict C cast.
 */

\set ON_ERROR_STOP on

DO
$$
DECLARE
  r           RECORD;
  _op         JSONB;
  _type       TEXT;
  _value      JSONB;
  _diff       BIGINT;
  _checked    INT := 0;
BEGIN
  FOR r IN
    SELECT op, is_hf01 FROM (VALUES
      -- ===== liquid transfers =====
      ('{"type":"transfer_operation","value":{"from":"admin","to":"steemit","amount":{"amount":"833000","precision":3,"nai":"@@000000021"},"memo":""}}', FALSE),
      ('{"type":"transfer_to_savings_operation","value":{"from":"abit","to":"abit","amount":{"amount":"1000","precision":3,"nai":"@@000000013"},"memo":""}}', FALSE),
      ('{"type":"fill_transfer_from_savings_operation","value":{"from":"abit","to":"abit","amount":{"amount":"1000","precision":3,"nai":"@@000000013"},"request_id":101,"memo":""}}', FALSE),
      ('{"type":"fill_recurrent_transfer_operation","value":{"from":"deathwing","to":"rishi556","amount":{"amount":"1000","precision":3,"nai":"@@000000021"},"memo":"test","remaining_executions":4}}', FALSE),

      -- ===== market orders =====
      ('{"type":"limit_order_create_operation","value":{"owner":"linouxis9","orderid":10,"amount_to_sell":{"amount":"9950","precision":3,"nai":"@@000000021"},"min_to_receive":{"amount":"3500","precision":3,"nai":"@@000000013"},"fill_or_kill":false,"expiration":"2035-10-29T06:32:22"}}', FALSE),
      ('{"type":"limit_order_create2_operation","value":{"owner":"dez1337","orderid":492991,"amount_to_sell":{"amount":"1","precision":3,"nai":"@@000000013"},"exchange_rate":{"base":{"amount":"1","precision":3,"nai":"@@000000013"},"quote":{"amount":"10","precision":3,"nai":"@@000000021"}},"fill_or_kill":false,"expiration":"2017-05-12T23:11:13"}}', FALSE),
      ('{"type":"limit_order_cancelled_operation","value":{"seller":"linouxis9","amount_back":{"amount":"9950","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"fill_order_operation","value":{"current_owner":"abit","current_orderid":42896,"current_pays":{"amount":"6105","precision":3,"nai":"@@000000013"},"open_owner":"nextgencrypto","open_orderid":1467589030,"open_pays":{"amount":"33000","precision":3,"nai":"@@000000021"}}}', FALSE),

      -- ===== conversions =====
      ('{"type":"convert_operation","value":{"owner":"summon","requestid":1467592156,"amount":{"amount":"5000","precision":3,"nai":"@@000000013"}}}', FALSE),
      ('{"type":"collateralized_convert_operation","value":{"owner":"gandalf","requestid":1625061900,"amount":{"amount":"1000","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"fill_convert_request_operation","value":{"owner":"summon","requestid":1467592156,"amount_in":{"amount":"5000","precision":3,"nai":"@@000000013"},"amount_out":{"amount":"18867","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"fill_collateralized_convert_request_operation","value":{"owner":"gandalf","requestid":1625061900,"amount_in":{"amount":"353","precision":3,"nai":"@@000000021"},"amount_out":{"amount":"103","precision":3,"nai":"@@000000013"},"excess_collateral":{"amount":"647","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"collateralized_convert_immediate_conversion_operation","value":{"owner":"gandalf","requestid":1625061900,"hbd_out":{"amount":"103","precision":3,"nai":"@@000000013"}}}', FALSE),

      -- ===== escrow (release / approved / rejected) =====
      ('{"type":"escrow_release_operation","value":{"from":"anonymtest","to":"someguy123","agent":"xtar","who":"xtar","receiver":"someguy123","escrow_id":72526562,"hbd_amount":{"amount":"5000","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"0","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"escrow_approved_operation","value":{"from":"anonymtest","to":"someguy123","agent":"xtar","escrow_id":72526562,"fee":{"amount":"1","precision":3,"nai":"@@000000013"}}}', FALSE),
      ('{"type":"escrow_rejected_operation","value":{"from":"hightouch","to":"fundition.help","agent":"ongame","escrow_id":1,"hbd_amount":{"amount":"1","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"1","precision":3,"nai":"@@000000021"},"fee":{"amount":"1","precision":3,"nai":"@@000000021"}}}', FALSE),

      -- ===== escrow_transfer: fee folded into matching symbol (fee in HIVE, then HBD) =====
      ('{"type":"escrow_transfer_operation","value":{"from":"hightouch","to":"fundition.help","hbd_amount":{"amount":"1","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"1","precision":3,"nai":"@@000000021"},"escrow_id":1,"agent":"ongame","fee":{"amount":"1","precision":3,"nai":"@@000000021"},"json_meta":"47700","ratification_deadline":"2018-11-06T04:05:33","escrow_expiration":"2018-11-07T04:05:33"}}', FALSE),
      ('{"type":"escrow_transfer_operation","value":{"from":"gregory.latinier","to":"ekitcho","hbd_amount":{"amount":"5","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"7","precision":3,"nai":"@@000000021"},"escrow_id":1,"agent":"fabien","fee":{"amount":"2","precision":3,"nai":"@@000000013"},"json_meta":"x","ratification_deadline":"2018-04-25T19:08:45","escrow_expiration":"2018-04-26T19:08:45"}}', FALSE),
      -- escrow_transfer where the HIVE leg is entirely fee (hive_amount 0) -> single HIVE row
      ('{"type":"escrow_transfer_operation","value":{"from":"gregory.latinier","to":"ekitcho","hbd_amount":{"amount":"1","precision":3,"nai":"@@000000013"},"hive_amount":{"amount":"0","precision":3,"nai":"@@000000021"},"escrow_id":1,"agent":"fabien","fee":{"amount":"1","precision":3,"nai":"@@000000021"},"json_meta":"x","ratification_deadline":"2018-04-25T19:08:45","escrow_expiration":"2018-04-26T19:08:45"}}', FALSE),

      -- ===== block / witness / liquidity / interest rewards =====
      ('{"type":"pow_reward_operation","value":{"worker":"admin","reward":{"amount":"21000","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"liquidity_reward_operation","value":{"owner":"adm","payout":{"amount":"1200000","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"interest_operation","value":{"owner":"hisnameisolllie","interest":{"amount":"1","precision":3,"nai":"@@000000013"},"is_saved_into_hbd_balance":true}}', FALSE),
      -- interest not saved into HBD balance -> emits nothing
      ('{"type":"interest_operation","value":{"owner":"hisnameisolllie","interest":{"amount":"1","precision":3,"nai":"@@000000013"},"is_saved_into_hbd_balance":false}}', FALSE),

      -- ===== producer reward (VESTS): both is_hf01 settings exercise x10^6 scaling =====
      ('{"type":"producer_reward_operation","value":{"producer":"initminer","vesting_shares":{"amount":"1000000","precision":6,"nai":"@@000000037"}}}', FALSE),
      ('{"type":"producer_reward_operation","value":{"producer":"initminer","vesting_shares":{"amount":"1000000","precision":6,"nai":"@@000000037"}}}', TRUE),

      -- ===== claim_reward_balance (HIVE + HBD + VESTS) =====
      ('{"type":"claim_reward_balance_operation","value":{"account":"ocrdu","reward_hive":{"amount":"17","precision":3,"nai":"@@000000021"},"reward_hbd":{"amount":"11","precision":3,"nai":"@@000000013"},"reward_vests":{"amount":"185025103","precision":6,"nai":"@@000000037"}}}', FALSE),
      ('{"type":"claim_reward_balance_operation","value":{"account":"ocrdu","reward_hive":{"amount":"17","precision":3,"nai":"@@000000021"},"reward_hbd":{"amount":"11","precision":3,"nai":"@@000000013"},"reward_vests":{"amount":"185025103","precision":6,"nai":"@@000000037"}}}', TRUE),

      -- ===== author / benefactor / curation rewards: payout_must_be_claimed false (emits) and true (empty) =====
      ('{"type":"author_reward_operation","value":{"author":"kaylinart","permlink":"x","hbd_payout":{"amount":"9048","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"5790","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"67826998226","precision":6,"nai":"@@000000037"},"curators_vesting_payout":{"amount":"16466162191","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":false}}', FALSE),
      ('{"type":"author_reward_operation","value":{"author":"kaylinart","permlink":"x","hbd_payout":{"amount":"9048","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"5790","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"67826998226","precision":6,"nai":"@@000000037"},"curators_vesting_payout":{"amount":"16466162191","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":false}}', TRUE),
      ('{"type":"author_reward_operation","value":{"author":"kaylinart","permlink":"x","hbd_payout":{"amount":"9048","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"5790","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"67826998226","precision":6,"nai":"@@000000037"},"curators_vesting_payout":{"amount":"16466162191","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":true}}', FALSE),
      ('{"type":"comment_benefactor_reward_operation","value":{"benefactor":"dpoll.curation","author":"sereze","permlink":"x","hbd_payout":{"amount":"27","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"2","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"118862104","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":false}}', FALSE),
      ('{"type":"comment_benefactor_reward_operation","value":{"benefactor":"dpoll.curation","author":"sereze","permlink":"x","hbd_payout":{"amount":"27","precision":3,"nai":"@@000000013"},"hive_payout":{"amount":"2","precision":3,"nai":"@@000000021"},"vesting_payout":{"amount":"118862104","precision":6,"nai":"@@000000037"},"payout_must_be_claimed":true}}', FALSE),
      ('{"type":"curation_reward_operation","value":{"curator":"steemroller","reward":{"amount":"2623363281","precision":6,"nai":"@@000000037"},"comment_author":"brookdemar","comment_permlink":"x","payout_must_be_claimed":false}}', FALSE),
      ('{"type":"curation_reward_operation","value":{"curator":"steemroller","reward":{"amount":"2623363281","precision":6,"nai":"@@000000037"},"comment_author":"brookdemar","comment_permlink":"x","payout_must_be_claimed":false}}', TRUE),
      ('{"type":"curation_reward_operation","value":{"curator":"steemroller","reward":{"amount":"2623363281","precision":6,"nai":"@@000000037"},"comment_author":"brookdemar","comment_permlink":"x","payout_must_be_claimed":true}}', FALSE),

      -- ===== vesting power-up / power-down completions (VESTS): both is_hf01 settings =====
      ('{"type":"fill_vesting_withdraw_operation","value":{"from_account":"randaletouri","to_account":"randaletouri","withdrawn":{"amount":"26475","precision":6,"nai":"@@000000037"},"deposited":{"amount":"710","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"fill_vesting_withdraw_operation","value":{"from_account":"randaletouri","to_account":"randaletouri","withdrawn":{"amount":"26475","precision":6,"nai":"@@000000037"},"deposited":{"amount":"710","precision":3,"nai":"@@000000021"}}}', TRUE),
      ('{"type":"transfer_to_vesting_completed_operation","value":{"from_account":"faddy","to_account":"faddy","hive_vested":{"amount":"357000","precision":3,"nai":"@@000000021"},"vesting_shares_received":{"amount":"357000000","precision":6,"nai":"@@000000037"}}}', FALSE),
      ('{"type":"transfer_to_vesting_completed_operation","value":{"from_account":"faddy","to_account":"faddy","hive_vested":{"amount":"357000","precision":3,"nai":"@@000000021"},"vesting_shares_received":{"amount":"357000000","precision":6,"nai":"@@000000037"}}}', TRUE),
      ('{"type":"account_created_operation","value":{"new_account_name":"witnesses","creator":"steem","initial_vesting_shares":{"amount":"72763034396","precision":6,"nai":"@@000000037"},"initial_delegation":{"amount":"220000000000","precision":6,"nai":"@@000000037"}}}', FALSE),
      ('{"type":"account_created_operation","value":{"new_account_name":"witnesses","creator":"steem","initial_vesting_shares":{"amount":"72763034396","precision":6,"nai":"@@000000037"},"initial_delegation":{"amount":"220000000000","precision":6,"nai":"@@000000037"}}}', TRUE),

      -- ===== account-creation fee burn (creator -> null) =====
      ('{"type":"claim_account_operation","value":{"creator":"almost-digital","fee":{"amount":"3000","precision":3,"nai":"@@000000021"},"extensions":[]}}', FALSE),
      ('{"type":"account_create_operation","value":{"fee":{"amount":"100000","precision":3,"nai":"@@000000021"},"creator":"murdock5","new_account_name":"proskynneo","owner":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM5sj5VtPtXr2UqJES3SGhPocFMTtm2SfTowfBEjNLuG51EUcmGb",1]]},"active":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM5sj5VtPtXr2UqJES3SGhPocFMTtm2SfTowfBEjNLuG51EUcmGb",1]]},"posting":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM5sj5VtPtXr2UqJES3SGhPocFMTtm2SfTowfBEjNLuG51EUcmGb",1]]},"memo_key":"STM5sj5VtPtXr2UqJES3SGhPocFMTtm2SfTowfBEjNLuG51EUcmGb","json_metadata":""}}', FALSE),
      ('{"type":"account_create_with_delegation_operation","value":{"fee":{"amount":"35000","precision":3,"nai":"@@000000021"},"delegation":{"amount":"220000000000","precision":6,"nai":"@@000000037"},"creator":"steem","new_account_name":"witnesses","owner":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM5YzwCee4R8Dxoj5SSnweGLLYA4qkZ9AQ8XxufRG2e3s5PWAkYD",1]]},"active":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM5hVs7ySn21sYrYmKUhukvo2myqKjiF8oHUau8dhMDGYqFNpJbJ",1]]},"posting":{"weight_threshold":1,"account_auths":[],"key_auths":[["STM89mZqrtnvrj3rsiPiUM34Zj1cNjaYzvSgQsmpa9rUXHPrdPrFL",1]]},"memo_key":"STM7gVcCwYM6UyhZaGRcpbpZi5rvKu79bS1AWaeBeBH5cevMbG7TA","json_metadata":"","extensions":[]}}', FALSE),

      -- ===== DHF / proposals =====
      ('{"type":"proposal_pay_operation","value":{"proposal_id":0,"receiver":"steem.dao","payer":"steem.dao","payment":{"amount":"157","precision":3,"nai":"@@000000013"}}}', FALSE),
      ('{"type":"proposal_fee_operation","value":{"creator":"gtg","treasury":"steem.dao","proposal_id":0,"fee":{"amount":"10000","precision":3,"nai":"@@000000013"}}}', FALSE),
      ('{"type":"dhf_funding_operation","value":{"treasury":"steem.dao","additional_funds":{"amount":"60","precision":3,"nai":"@@000000013"}}}', FALSE),
      ('{"type":"dhf_conversion_operation","value":{"treasury":"hive.fund","hive_amount_in":{"amount":"41676736","precision":3,"nai":"@@000000021"},"hbd_amount_out":{"amount":"6543247","precision":3,"nai":"@@000000013"}}}', FALSE),

      -- ===== hardfork balance migrations =====
      ('{"type":"hardfork_hive_operation","value":{"account":"steemit","treasury":"steem.dao","other_affected_accounts":[],"hbd_transferred":{"amount":"100","precision":3,"nai":"@@000000013"},"hive_transferred":{"amount":"200","precision":3,"nai":"@@000000021"},"vests_converted":{"amount":"4000","precision":6,"nai":"@@000000037"},"total_hive_from_vests":{"amount":"300","precision":3,"nai":"@@000000021"}}}', FALSE),
      ('{"type":"hardfork_hive_restore_operation","value":{"account":"angelina6688","treasury":"steem.dao","hbd_transferred":{"amount":"25","precision":3,"nai":"@@000000013"},"hive_transferred":{"amount":"2787","precision":3,"nai":"@@000000021"}}}', FALSE),

      -- ===== treasury consolidation: total_moved is an array of assets =====
      ('{"type":"consolidate_treasury_balance_operation","value":{"total_moved":[{"amount":"83353473585","precision":3,"nai":"@@000000021"},{"amount":"560371025","precision":3,"nai":"@@000000013"}]}}', FALSE)
    ) AS f(op, is_hf01)
  LOOP
    _op    := r.op::JSONB;
    _type  := _op ->> 'type';
    _value := _op -> 'value';

    SELECT count(*) INTO _diff FROM (
      (
        SELECT account_name, amount, asset_precision, asset_symbol_nai
        FROM hive.get_impacted_balances(_op::hafd.operation, r.is_hf01)
        EXCEPT ALL
        SELECT account_name, amount, asset_precision, asset_symbol_nai
        FROM btracker_backend.get_impacted_balances(_type, _value, r.is_hf01)
      )
      UNION ALL
      (
        SELECT account_name, amount, asset_precision, asset_symbol_nai
        FROM btracker_backend.get_impacted_balances(_type, _value, r.is_hf01)
        EXCEPT ALL
        SELECT account_name, amount, asset_precision, asset_symbol_nai
        FROM hive.get_impacted_balances(_op::hafd.operation, r.is_hf01)
      )
    ) d;

    IF _diff <> 0 THEN
      RAISE EXCEPTION 'impacted_balances parity mismatch for "%" (is_hf01=%): % differing rows between C and SQL implementations',
        _type, r.is_hf01, _diff;
    END IF;

    _checked := _checked + 1;
  END LOOP;

  -- The five overloads below are intentionally empty in forward_impacted.cpp (the real
  -- balance movement is recorded by a paired virtual operation). Confirm the SQL port
  -- has no stray branch for them: any body must yield zero rows.
  FOR r IN
    SELECT t AS type_name FROM unnest(ARRAY[
      'clear_null_account_balance_operation',
      'escrow_approve_operation',
      'transfer_to_vesting_operation',
      'transfer_from_savings_operation',
      'limit_order_cancel_operation'
    ]) AS t
  LOOP
    PERFORM 1 FROM btracker_backend.get_impacted_balances(r.type_name, '{}'::JSONB, FALSE);
    IF FOUND THEN
      RAISE EXCEPTION 'impacted_balances parity: "%" must be a no-op but the SQL implementation emitted rows', r.type_name;
    END IF;
    _checked := _checked + 1;
  END LOOP;

  RAISE NOTICE 'impacted_balances parity: all % fixtures match', _checked;
END
$$;
