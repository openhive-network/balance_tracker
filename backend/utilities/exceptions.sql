SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.validate_limit(given_limit BIGINT, expected_limit INT,given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_limit > expected_limit THEN
    RAISE EXCEPTION '% <= %: % of % is greater than maxmimum allowed', given_limit_name, expected_limit, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.validate_page(given_page BIGINT, max_page INT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_page > max_page AND given_page != 1 THEN
    RAISE EXCEPTION 'page <= %: page of % is greater than maxmimum page', max_page, given_page;
  END IF;

  RETURN;
END
$$;


CREATE OR REPLACE FUNCTION btracker_backend.validate_negative_limit(given_limit BIGINT, given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_limit <= 0 THEN
    RAISE EXCEPTION '% <= 0: % of % is lesser or equal 0', given_limit_name, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.validate_negative_page(given_page BIGINT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF given_page <= 0 THEN
    RAISE EXCEPTION 'page <= 0: page of % is lesser or equal 0', given_page;
  END IF;

  RETURN;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.rest_raise_missing_account(_account_name TEXT)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  RAISE EXCEPTION 'Account ''%'' does not exist', _account_name;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.rest_raise_vest_saving_balance(
    _balance_type btracker_backend.balance_type,
    _nai_type btracker_backend.nai_type
)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  RAISE EXCEPTION 'The selected balance type ''%'' does not support ''%'' option. Please choose a different balance type.',
   _balance_type::TEXT, _nai_type::TEXT;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.validate_block(
  block_num BIGINT,
  block_name TEXT DEFAULT 'block'
)
RETURNS VOID
LANGUAGE 'plpgsql'
IMMUTABLE
AS
$$
BEGIN
  IF block_num <= 0 THEN
    RAISE EXCEPTION '% must be greater than zero. Given %', block_name, block_num;
  END IF;
  RETURN;
END
$$;

RESET ROLE;
