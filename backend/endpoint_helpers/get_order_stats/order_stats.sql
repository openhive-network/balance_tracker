SET ROLE btracker_owner;

/*
Blocks per row of order_stats_by_block_group. A network-wide window reads at
most this many order_stats_by_block rows at each edge, and one group row per
this many blocks between them.

The value is baked into the block_group keys order_stats_by_block_group stores
(db/order_stats_rollups.sql seeds them with it): changing it requires rebuilding
order_stats_by_block_group.
*/
CREATE OR REPLACE FUNCTION btracker_backend.order_stats_group_blocks()
RETURNS INT
LANGUAGE sql
IMMUTABLE
AS $BODY$
  SELECT 10000
$BODY$;

/*
Lifecycle counts of the limit orders created in blocks _from_block.._to_block
(either bound NULL: unbounded), of one owner or, when _account_id is NULL, of
every account. Outcomes count whenever they happened.

One owner's orders are counted from order_lifecycle through its owner and
creation-block index. Every account's are summed from the rollups block
processing keeps of order_lifecycle: the whole block groups inside the window
from order_stats_by_block_group, the blocks before the first and after the last
of them from order_stats_by_block, so the cost does not grow with the window.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_order_stats(
    _account_id INT,
    _from_block INT,
    _to_block INT
)
RETURNS btracker_backend.order_stats
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
  __from INT := COALESCE(_from_block, 0);
  __to   INT := COALESCE(_to_block, 2147483647);
  __group_blocks CONSTANT INT := btracker_backend.order_stats_group_blocks();
  -- BIGINT: the bounds of an unbounded window overflow INT.
  __first_group BIGINT := (__from::BIGINT + __group_blocks - 1) / __group_blocks;
  __last_group  BIGINT := (__to::BIGINT + 1) / __group_blocks - 1;
  __head_to     BIGINT := LEAST(__to, __first_group * __group_blocks - 1);
  __tail_from   BIGINT := GREATEST(__from, GREATEST(__first_group, __last_group + 1) * __group_blocks);
  __created   INT;
  __filled    INT;
  __cancelled INT;
  __open      INT;
BEGIN
  IF _account_id IS NULL THEN
    SELECT
      COALESCE(SUM(r.created), 0),
      COALESCE(SUM(r.filled), 0),
      COALESCE(SUM(r.cancelled), 0)
    INTO __created, __filled, __cancelled
    FROM (
      SELECT g.created, g.filled, g.cancelled
      FROM order_stats_by_block_group g
      WHERE g.block_group BETWEEN __first_group AND __last_group
      UNION ALL
      SELECT b.created, b.filled, b.cancelled
      FROM order_stats_by_block b
      WHERE b.block_created BETWEEN __from AND __head_to
      UNION ALL
      SELECT b.created, b.filled, b.cancelled
      FROM order_stats_by_block b
      WHERE b.block_created BETWEEN __tail_from AND __to
    ) r;
    __open := __created - __filled - __cancelled;
  ELSE
    SELECT
      COUNT(*),
      COUNT(*) FILTER (WHERE ol.outcome = 'filled'),
      COUNT(*) FILTER (WHERE ol.outcome = 'cancelled'),
      COUNT(*) FILTER (WHERE ol.outcome IS NULL)
    INTO __created, __filled, __cancelled, __open
    FROM order_lifecycle ol
    WHERE ol.owner_id = _account_id
      AND ol.block_created BETWEEN __from AND __to;
  END IF;

  RETURN (
    __created,
    __filled,
    __cancelled,
    __open,
    CASE WHEN __created > 0 THEN round(100.0 * __filled / __created, 2)::FLOAT END
  )::btracker_backend.order_stats;
END
$BODY$;

RESET ROLE;
