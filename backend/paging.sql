SET ROLE btracker_owner;

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

  __count := (
      SELECT COUNT(*)
      FROM account_balance_history 
      WHERE 
        account = _account_id AND 
        nai = _coin_type AND
        source_op < __to_op AND
        source_op >= __from_op
  );
  
  RETURN (__count, __from_op, __to_op)::btracker_backend.balance_history_range_return;
END
$$;

RESET ROLE;
