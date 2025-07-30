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

RESET ROLE;
