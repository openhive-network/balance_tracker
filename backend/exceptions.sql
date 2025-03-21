SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION validate_limit(given_limit BIGINT, expected_limit INT,given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF given_limit > expected_limit THEN
    RAISE EXCEPTION '% <= %: % of % is greater than maxmimum allowed', given_limit_name, expected_limit, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

CREATE OR REPLACE FUNCTION validate_page(given_page BIGINT, max_page INT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF given_page > max_page AND given_page != 1 THEN
    RAISE EXCEPTION 'page <= %: page of % is greater than maxmimum page', max_page, given_page;
  END IF;

  RETURN;
END
$$;


CREATE OR REPLACE FUNCTION validate_negative_limit(given_limit BIGINT, given_limit_name TEXT DEFAULT 'page-size')
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF given_limit <= 0 THEN
    RAISE EXCEPTION '% <= 0: % of % is lesser or equal 0', given_limit_name, given_limit_name, given_limit;
  END IF;

  RETURN;
END
$$;

CREATE OR REPLACE FUNCTION validate_negative_page(given_page BIGINT)
RETURNS VOID -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
BEGIN
  IF given_page <= 0 THEN
    RAISE EXCEPTION 'page <= 0: page of % is lesser or equal 0', given_page;
  END IF;

  RETURN;
END
$$;

RESET ROLE;
