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
