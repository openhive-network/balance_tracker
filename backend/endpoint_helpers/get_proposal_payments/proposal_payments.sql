SET ROLE btracker_owner;

/*
DHF proposal payment totals and history.
Called by: btracker_endpoints.get_proposals_payments(), btracker_endpoints.get_proposal_payments()

Both read what process_block_range_proposal_payments keeps:
  * proposal_payment_totals: one row per paid proposal, point reads by proposal_id;
  * proposal_payments: one row per payment, paged by (proposal_id, payment_seq_no).
    payment_count in the totals row is the highest payment_seq_no, so a page is a
    seq_no window on the primary key, like balance and vesting history.
*/

-- Comma-separated proposal ids -> distinct ids in ascending order.
CREATE OR REPLACE FUNCTION btracker_backend.parse_proposal_ids(_proposal_ids TEXT)
RETURNS INT[]
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __items TEXT[] := string_to_array(_proposal_ids, ',');
  __bad   TEXT;
BEGIN
  IF _proposal_ids IS NULL THEN
    RAISE EXCEPTION 'proposal-ids must not be null';
  END IF;

  PERFORM btracker_backend.validate_limit(cardinality(__items), 1000, 'proposal-ids');

  SELECT i INTO __bad
  FROM unnest(__items) AS i
  WHERE CASE WHEN btrim(i) ~ '^[0-9]{1,10}$' THEN btrim(i)::BIGINT > 2147483647 ELSE TRUE END
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'proposal-ids must be a comma-separated list of non-negative integers: ''%'' is not one', __bad;
  END IF;

  RETURN ARRAY(SELECT DISTINCT btrim(i)::INT FROM unnest(__items) AS i ORDER BY 1);
END
$$;

CREATE OR REPLACE FUNCTION btracker_backend.validate_proposal_id(_proposal_id INT)
RETURNS VOID
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  IF _proposal_id IS NULL OR _proposal_id < 0 THEN
    RAISE EXCEPTION 'proposal-id must be a non-negative integer: %', _proposal_id;
  END IF;
END
$$;

-- One row per requested id; a proposal never paid gets a zero total and nulls.
CREATE OR REPLACE FUNCTION btracker_backend.get_proposal_payment_totals(_proposal_ids INT[])
RETURNS SETOF btracker_backend.proposal_payment_totals
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
AS
$$
BEGIN
  RETURN QUERY
  SELECT
    ids.proposal_id,
    av.name::TEXT,
    btracker_backend.create_amount_object(btracker_backend.nai_hbd(), COALESCE(t.total_paid, 0)),
    COALESCE(t.payment_count, 0),
    t.first_payment_block,
    t.first_payment_at,
    t.last_payment_block,
    t.last_payment_at
  FROM unnest(_proposal_ids) AS ids(proposal_id)
  LEFT JOIN proposal_payment_totals t ON t.proposal_id = ids.proposal_id
  LEFT JOIN hive.accounts_view av ON av.id = t.receiver
  ORDER BY ids.proposal_id;
END
$$;

/*
plan_cache_mode = force_custom_plan: the page statement picks its scan direction
and seq_no bound from _direction and the page; a generic plan sees neither (see
get_account_vesting_history).
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_proposal_payment_history(
    _proposal_id INT,
    _page        INT,
    _page_size   INT,
    _direction   btracker_backend.sort_direction
)
RETURNS btracker_backend.proposal_payment_history
LANGUAGE 'plpgsql' STABLE
SET jit = OFF
SET plan_cache_mode = 'force_custom_plan'
AS
$$
DECLARE
  _count  INT;
  _calc   btracker_backend.calculate_pages_return;
  _result btracker_backend.proposal_payment[];
BEGIN
  SELECT COALESCE(MAX(t.payment_count), 0) INTO _count
  FROM proposal_payment_totals t
  WHERE t.proposal_id = _proposal_id;

  _calc := btracker_backend.calculate_pages(_count, _page, _direction, _page_size);

  WITH gather_page AS MATERIALIZED (
    SELECT
      pp.payment_seq_no,
      pp.amount,
      pp.source_op,
      hafd.operation_id_to_block_num(pp.source_op) AS block_num
    FROM proposal_payments pp
    WHERE pp.proposal_id = _proposal_id
      AND (_direction = 'desc' OR pp.payment_seq_no > _calc.offset_filter)
      AND (_direction = 'asc'  OR pp.payment_seq_no <= _count - _calc.offset_filter)
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN pp.payment_seq_no ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN pp.payment_seq_no ELSE NULL END) ASC
    LIMIT _calc.limit_filter
  )
  SELECT array_agg(
    (
      gp.block_num,
      gp.source_op::TEXT,
      btracker_backend.create_amount_object(btracker_backend.nai_hbd(), gp.amount),
      bv.created_at
    )::btracker_backend.proposal_payment
    ORDER BY
      (CASE WHEN _direction = 'desc' THEN gp.payment_seq_no ELSE NULL END) DESC,
      (CASE WHEN _direction = 'asc'  THEN gp.payment_seq_no ELSE NULL END) ASC
  )
  INTO _result
  FROM gather_page gp
  JOIN hive.blocks_view bv ON bv.num = gp.block_num;

  RETURN (
    _count,
    COALESCE(_calc.total_pages, 0),
    COALESCE(_result, '{}'::btracker_backend.proposal_payment[])
  )::btracker_backend.proposal_payment_history;
END
$$;

RESET ROLE;
