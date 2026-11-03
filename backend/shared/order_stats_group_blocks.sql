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

RESET ROLE;
