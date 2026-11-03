SET ROLE btracker_owner;

/*
Lifecycle counts of the limit orders created in blocks _from_block.._to_block
(either bound NULL: unbounded), of one owner or, when _account_id is NULL, of
every account. Outcomes count whenever they happened.

The two branches are separate statements so that each plans with its own index
(owner and creation block, or creation block alone), and both read only the
indexed columns.
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
  __created   INT;
  __filled    INT;
  __cancelled INT;
  __open      INT;
BEGIN
  IF _account_id IS NULL THEN
    SELECT
      COUNT(*),
      COUNT(*) FILTER (WHERE ol.outcome = 'filled'),
      COUNT(*) FILTER (WHERE ol.outcome = 'cancelled'),
      COUNT(*) FILTER (WHERE ol.outcome IS NULL)
    INTO __created, __filled, __cancelled, __open
    FROM order_lifecycle ol
    WHERE ol.block_created BETWEEN __from AND __to;
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
