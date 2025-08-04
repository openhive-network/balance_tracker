SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_backend.total_pages(
    _ops_count INT,
    _page_size INT
)
RETURNS INT -- noqa: LT01, CP05
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN (
    CASE 
      WHEN (_ops_count % _page_size) = 0 THEN 
        _ops_count / _page_size 
      ELSE 
        (_ops_count / _page_size) + 1
    END
  );
END
$$;

DROP TYPE IF EXISTS btracker_backend.calculate_pages_return CASCADE;
CREATE TYPE btracker_backend.calculate_pages_return AS
(
    rest_of_division INT,
    total_pages INT,
    page_num INT,
    offset_filter INT,
    limit_filter INT
);

CREATE OR REPLACE FUNCTION btracker_backend.calculate_pages(
    _count INT,
    _page INT,
    _order_is btracker_backend.sort_direction, -- noqa: LT01, CP05
    _limit INT
)
RETURNS btracker_backend.calculate_pages_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
DECLARE 
  __rest_of_division INT;
  __total_pages INT;
  __page INT;
  __offset INT;
  __limit INT;
BEGIN
  __rest_of_division := (_count % _limit)::INT;

  __total_pages := (
    CASE 
      WHEN (__rest_of_division = 0) THEN 
        _count / _limit 
      ELSE 
        (_count / _limit) + 1
      END
  )::INT;

  __page := (
    CASE 
      WHEN (_page IS NULL) THEN 
        1
      WHEN (_page IS NOT NULL) AND _order_is = 'desc' THEN 
        __total_pages - _page + 1
      ELSE 
        _page 
      END
  );

  __offset := (
    CASE
      WHEN _order_is = 'desc' AND __page != 1 AND __rest_of_division != 0 THEN 
        ((__page - 2) * _limit) + __rest_of_division
      WHEN __page = 1 THEN 
        0
      ELSE
        (__page - 1) * _limit
      END
    );

  __limit := (
      CASE
        WHEN _order_is = 'desc' AND __page = 1             AND __rest_of_division != 0 THEN
          __rest_of_division 
        WHEN _order_is = 'asc'  AND __page = __total_pages AND __rest_of_division != 0 THEN
          __rest_of_division 
        ELSE 
          _limit 
        END
    );

  PERFORM btracker_backend.validate_page(_page, __total_pages);

  RETURN (__rest_of_division, __total_pages, __page, __offset, __limit)::btracker_backend.calculate_pages_return;
END
$$;

DROP TYPE IF EXISTS btracker_backend.balance_history_range_return CASCADE;
CREATE TYPE btracker_backend.balance_history_range_return AS
(
    count INT,
    from_op BIGINT,
    to_op BIGINT
);

CREATE OR REPLACE FUNCTION btracker_backend.balance_history_range(
    _account_id INT,
    _coin_type INT,
    _balance_type btracker_backend.balance_type,
    _from INT, 
    _to INT
)
RETURNS btracker_backend.balance_history_range_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
DECLARE 
  __to_op BIGINT;
  __from_op BIGINT;
  __count INT;
BEGIN
  __to_op := (
    SELECT 
      ov.id
		FROM hive.operations_view ov
		WHERE 
      (_to IS NULL OR ov.block_num <= _to)
		ORDER BY ov.block_num DESC, ov.id DESC LIMIT 1
  );

  __from_op := (
    SELECT 
      ov.id 
		FROM hive.operations_view ov
		WHERE 
      (_from IS NULL OR ov.block_num >= _from)
		ORDER BY ov.block_num, ov.id
		LIMIT 1
  );

    __count := CASE 
      WHEN _balance_type = 'balance' THEN
        (
          SELECT COUNT(*)
          FROM account_balance_history 
          WHERE 
            account = _account_id AND 
            nai = _coin_type AND
            source_op < __to_op AND
            source_op >= __from_op
        )
      ELSE
        (
          SELECT COUNT(*)
          FROM account_savings_history 
          WHERE 
            account = _account_id AND 
            nai = _coin_type AND
            source_op < __to_op AND
            source_op >= __from_op
        )
      END;
    
  RETURN (__count, __from_op, __to_op)::btracker_backend.balance_history_range_return;
END
$$;

DROP TYPE IF EXISTS btracker_backend.paging_return CASCADE;
CREATE TYPE btracker_backend.paging_return AS
(
    from_block INT,
    to_block INT
);

CREATE OR REPLACE FUNCTION btracker_backend.block_range(
    _from INT, 
    _to INT,
    _current_block INT
)
RETURNS btracker_backend.paging_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' IMMUTABLE
SET JIT = OFF
AS
$$
DECLARE 
  __to INT;
  __from INT;
BEGIN
  __to := (
    CASE 
      WHEN (_to IS NULL) THEN 
        _current_block 
      WHEN (_to IS NOT NULL) AND (_current_block < _to) THEN 
        _current_block 
      ELSE 
        _to 
      END
  );

  __from := (
    CASE 
      WHEN (_from IS NULL) THEN 
        1 
      ELSE 
        _from 
      END
  );

  RETURN (__from, __to)::btracker_backend.paging_return;
END
$$;

RESET ROLE;
