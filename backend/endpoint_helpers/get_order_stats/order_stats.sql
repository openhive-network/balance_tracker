SET ROLE btracker_owner;

-- Both REST endpoints use this one creation cohort and outcome calculation.
-- Date bounds select creations; terminal outcomes are evaluated at the index
-- watermark rather than at the upper creation-date bound.
CREATE OR REPLACE FUNCTION btracker_backend.get_order_stats(
    _account_id INT = NULL,
    _from_date TIMESTAMP = NULL,
    _to_date TIMESTAMP = NULL
)
RETURNS btracker_backend.order_stats
LANGUAGE plpgsql STABLE
SET jit = OFF
SET plan_cache_mode = force_custom_plan
AS
$$
DECLARE
  _processed_through INT;
  _backfill_required BOOLEAN;
  _context_head INT;
  _indexed_at TIMESTAMP;
  _result btracker_backend.order_stats;
BEGIN
  IF _from_date > _to_date THEN
    RAISE EXCEPTION 'from-date must be less than or equal to to-date'
      USING ERRCODE = 'PT400';
  END IF;

  SELECT
    p.processed_through,
    p.backfill_required,
    p.context_head,
    p.indexed_at
  INTO
    _processed_through,
    _backfill_required,
    _context_head,
    _indexed_at
  FROM btracker_backend.order_lifecycle_progress_view p;

  IF NOT FOUND
    OR _backfill_required IS DISTINCT FROM FALSE
    OR _processed_through IS NULL
    OR _context_head IS NULL
    OR _processed_through <> _context_head
  THEN
    RAISE EXCEPTION 'Complete order lifecycle history is not ready through the application head'
      USING ERRCODE = 'PT503';
  END IF;

  WITH cohort_counts AS (
    SELECT
      COUNT(*) AS created,
      COUNT(*) FILTER (
        WHERE l.outcome = 'filled' AND l.terminal_block <= _processed_through
      ) AS filled,
      COUNT(*) FILTER (
        WHERE l.outcome = 'canceled' AND l.terminal_block <= _processed_through
      ) AS canceled
    FROM btracker_backend.order_lifecycle_view l
    WHERE l.block_created <= _processed_through
      AND (_account_id IS NULL OR l.owner_id = _account_id)
      AND (_from_date IS NULL OR l.created_at >= _from_date)
      AND (_to_date IS NULL OR l.created_at <= _to_date)
  )
  SELECT
    c.created,
    c.filled,
    c.canceled,
    c.created - c.filled - c.canceled,
    ROUND(100.0 * c.filled / NULLIF(c.created, 0), 3),
    _processed_through,
    _indexed_at,
    1
  INTO _result
  FROM cohort_counts c;

  RETURN _result;
END
$$;

RESET ROLE;
