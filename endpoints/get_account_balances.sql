SET ROLE btracker_owner;

CREATE SCHEMA IF NOT EXISTS btracker_endpoints AUTHORIZATION btracker_owner;

DROP TYPE IF EXISTS btracker_endpoints.account_info_rewards CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.account_info_rewards AS
(
    curation_rewards BIGINT,
    posting_rewards BIGINT
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_info_rewards(_account int)
RETURNS btracker_endpoints.account_info_rewards -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  _result btracker_endpoints.account_info_rewards;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT cv.curation_rewards, cv.posting_rewards
INTO _result
FROM account_info_rewards cv
WHERE cv.account = _account;

IF NOT FOUND THEN 
  _result = (0::BIGINT, 0::BIGINT);
END IF;

RETURN _result;
END
$$;

DROP TYPE IF EXISTS btracker_endpoints.account_savings CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.account_savings AS
(
    hbd_savings numeric,
    hive_savings numeric,
    savings_withdraw_requests INT
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_savings(_account int)
RETURNS btracker_endpoints.account_savings -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
SET SEARCH_PATH = :BTRACKER_SCHEMA -- noqa: LT01
AS
$$
DECLARE
  __result btracker_endpoints.account_savings;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN saving_balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN saving_balance END) AS hive
INTO __result.hbd_savings, __result.hive_savings
FROM account_savings WHERE account= _account;

SELECT SUM (savings_withdraw_requests) AS total
INTO __result.savings_withdraw_requests
FROM account_savings
WHERE account= _account;

RETURN __result;
END
$$;

DROP TYPE IF EXISTS btracker_endpoints.account_rewards CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.account_rewards AS
(
    hbd_rewards numeric,
    hive_rewards numeric,
    vests_rewards numeric,
    hive_vesting_rewards numeric
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_rewards(_account int)
RETURNS btracker_endpoints.account_rewards -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
SET SEARCH_PATH = :BTRACKER_SCHEMA -- noqa: LT01
AS
$$
DECLARE
  __result btracker_endpoints.account_rewards;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN balance END) AS hive,
  MAX(CASE WHEN nai = 37 THEN balance END) AS vests,
  MAX(CASE WHEN nai = 38 THEN balance END) AS vesting_hive
INTO __result
FROM account_rewards WHERE account= _account;

RETURN __result;
END
$$;


DROP TYPE IF EXISTS btracker_endpoints.btracker_vests_balance CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.btracker_vests_balance AS
(
    delegated_vests BIGINT,
    received_vests BIGINT
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_delegations(_account int)
RETURNS btracker_endpoints.btracker_vests_balance -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
SET SEARCH_PATH = :BTRACKER_SCHEMA -- noqa: LT01
AS
$$
DECLARE
  _result btracker_endpoints.btracker_vests_balance;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT cv.delegated_vests, cv.received_vests
INTO _result
FROM account_delegations cv WHERE cv.account = _account;

IF NOT FOUND THEN 
  _result = (0::BIGINT, 0::BIGINT);
END IF;

RETURN _result;
END
$$;

DROP TYPE IF EXISTS btracker_endpoints.account_withdraws CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.account_withdraws AS
(
    vesting_withdraw_rate BIGINT,
    to_withdraw BIGINT,
    withdrawn BIGINT,
    withdraw_routes INT,
    delayed_vests BIGINT
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_withdraws(_account int)
RETURNS btracker_endpoints.account_withdraws -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
SET SEARCH_PATH = :BTRACKER_SCHEMA -- noqa: LT01
AS
$$
DECLARE
  __result btracker_endpoints.account_withdraws;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT vesting_withdraw_rate, to_withdraw, withdrawn, withdraw_routes, delayed_vests
INTO __result
FROM account_withdraws WHERE account= _account;
RETURN __result;
END
$$;


--ACCOUNT HIVE, HBD, VEST BALANCES

DROP TYPE IF EXISTS btracker_endpoints.btracker_account_balance CASCADE; -- noqa: LT01
CREATE TYPE btracker_endpoints.btracker_account_balance AS
(
    hbd_balance BIGINT,
    hive_balance BIGINT,
    vesting_shares BIGINT,
    vesting_balance_hive BIGINT,
    post_voting_power_vests BIGINT
);

CREATE OR REPLACE FUNCTION btracker_endpoints.get_account_balances(_account int)
RETURNS btracker_endpoints.btracker_account_balance -- noqa: LT01
LANGUAGE 'plpgsql'
STABLE
SET SEARCH_PATH = :BTRACKER_SCHEMA -- noqa: LT01
AS
$$
DECLARE
  __result btracker_endpoints.btracker_account_balance;
BEGIN

PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

SELECT  
  MAX(CASE WHEN nai = 13 THEN balance END) AS hbd,
  MAX(CASE WHEN nai = 21 THEN balance END) AS hive, 
  MAX(CASE WHEN nai = 37 THEN balance END) AS vest 
INTO __result
FROM current_account_balances WHERE account= _account;

SELECT hive.get_vesting_balance((SELECT num FROM hive.blocks_view ORDER BY num DESC LIMIT 1), __result.vesting_shares) 
INTO __result.vesting_balance_hive;

SELECT (__result.vesting_shares - delegated_vests + received_vests) 
INTO __result.post_voting_power_vests
FROM btracker_endpoints.get_account_delegations(_account);

RETURN __result;

END
$$;

RESET ROLE;
