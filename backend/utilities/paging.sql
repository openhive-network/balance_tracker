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
    from_seq INT,
    to_seq INT
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
BEGIN
  IF _balance_type = 'balance' THEN
    RETURN btracker_backend.bh_balance(_account_id, _coin_type, _from, _to);
  END IF;

  RETURN btracker_backend.bh_savings_balance(_account_id, _coin_type, _from, _to);
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.bh_balance(
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
  __to_seq INT;
  __from_seq INT;
BEGIN
  IF _to IS NULL THEN
    __to_seq := (
      SELECT
        ab.balance_seq_no
      FROM account_balance_history ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no DESC LIMIT 1
    );
  ELSE
    __to_seq := (
      WITH last_block AS (
        SELECT
          ab.source_op_block
        FROM account_balance_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block <= _to
        ORDER BY ab.source_op_block DESC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM account_balance_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM last_block)
      )
      SELECT MAX(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  IF _from IS NULL THEN
    __from_seq := (
      SELECT
        ab.balance_seq_no
      FROM account_balance_history ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no ASC LIMIT 1
    );
  ELSE
    __from_seq := (
      WITH first_block AS (
        SELECT
          ab.source_op_block
        FROM account_balance_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block >= _from
        ORDER BY ab.source_op_block ASC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM account_balance_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM first_block)
      )
      SELECT MIN(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  RETURN (
    COALESCE((__to_seq - __from_seq + 1), 0),
    COALESCE(__from_seq, 0),
    COALESCE(__to_seq, 0)
  )::btracker_backend.balance_history_range_return;
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.bh_savings_balance(
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
  __to_seq INT;
  __from_seq INT;
BEGIN
  IF _to IS NULL THEN
    __to_seq := (
      SELECT
        ab.balance_seq_no
      FROM account_savings_history ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no DESC LIMIT 1
    );
  ELSE
    __to_seq := (
      WITH last_block AS (
        SELECT
          ab.source_op_block
        FROM account_savings_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block <= _to
        ORDER BY ab.source_op_block DESC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM account_savings_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM last_block)
      )
      SELECT MAX(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  IF _from IS NULL THEN
    __from_seq := (
      SELECT
        ab.balance_seq_no
      FROM account_savings_history ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no ASC LIMIT 1
    );
  ELSE
    __from_seq := (
      WITH first_block AS (
        SELECT
          ab.source_op_block
        FROM account_savings_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block >= _from
        ORDER BY ab.source_op_block ASC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM account_savings_history ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM first_block)
      )
      SELECT MIN(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  RETURN (
    COALESCE((__to_seq - __from_seq + 1), 0),
    COALESCE(__from_seq, 0),
    COALESCE(__to_seq, 0)
  )::btracker_backend.balance_history_range_return;
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
