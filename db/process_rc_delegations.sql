SET ROLE btracker_owner;

/**
 * process_block_range_rc_delegations - Process RC delegation operations for a block range
 *
 * OPERATIONS PROCESSED:
 *   - custom_json_operation with id='rc' containing delegate_rc_operation
 *   - withdraw_vesting, fill_vesting_withdraw and delegate_vesting_shares of an
 *     account with outgoing RC delegations: hived's implicit reduction of those
 *     delegations when the account's vesting falls below what it delegated
 *     (see process_rc_delegation_range)
 *
 * HARDFORK REQUIREMENT:
 *   RC delegations were formalized in HF26 (block ~68,676,505).
 *   This function skips processing for blocks before HF26.
 *
 * ORDER IN THE BATCH:
 *   Runs after process_block_range_balances (it reads the batch's VESTS changes
 *   that step stages in _btracker_vests_batch, and current_account_balances as of
 *   _to) and before process_block_range_withdrawals and
 *   process_block_range_delegations (it reads account_withdraws,
 *   account_delegations and current_accounts_delegations as of _from - 1).
 */
CREATE OR REPLACE FUNCTION process_block_range_rc_delegations(IN _from INT, IN _to INT, IN _report_step INT = 1000)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  -- HF26 block (RC delegations were formalized in HF26)
  __hardfork_26_block INT;
BEGIN
------------------------------------------------------------------------------
-- HF26 CHECK: RC delegations were formalized in HF26
--
-- Skip processing if block range is entirely before HF26.
-- If range spans HF26, RC operations are processed from the HF26 block on,
-- while the vesting state they are checked against is replayed from _from.
------------------------------------------------------------------------------
SELECT block_num INTO __hardfork_26_block
FROM hafd.applied_hardforks
WHERE hardfork_num = btracker_backend.hf_rc_delegations();

-- If HF26 hasn't been applied yet (e.g., during early sync), skip entirely
IF __hardfork_26_block IS NULL OR _to < __hardfork_26_block THEN
  RETURN;
END IF;

PERFORM process_rc_delegation_range(_from, GREATEST(_from, __hardfork_26_block), _to);
END
$$;

/**
 * rc_delegation_vest_checks(_batch_from, _from, _to)
 * --------------------------------------------------
 * Every point in blocks _from.._to where hived checks an account's RC delegations
 * against its vesting (resource_credits::update_account_after_vest_change with the
 * overflow check), for the accounts that have outgoing RC delegations before the
 * batch or delegate RC in it:
 *   - withdraw_vesting: the account;
 *   - delegate_vesting_shares: the delegator and the delegatee;
 *   - a weekly power-down payout: the account, after its last fill_vesting_withdraw
 *     of the block (hived checks once, after all the payout's routes).
 *
 * effective_vests is hived's get_effective_vesting_shares() right after that
 * operation:
 *   vesting_shares - delegated_vesting_shares + received_vesting_shares
 *   - next_vesting_withdrawal (min(rate, to_withdraw - withdrawn) while powering down)
 * (a power-down ends at a payout that leaves the account with 0 VESTS, as in hived's
 * process_vesting_withdrawals, so its next_vesting_withdrawal is 0 from that payout on)
 * replayed from the state before the batch (account_withdraws, account_delegations,
 * current_accounts_delegations, which the withdrawals and delegations steps have not
 * updated yet) through the batch's operations from _batch_from, and the VESTS
 * balance at the operation (current_account_balances minus the batch's later
 * changes staged in _btracker_vests_batch).
 *
 * Reads the temp table _rc_delegation_ops (the batch's parsed delegate_rc ops).
 */
CREATE OR REPLACE FUNCTION rc_delegation_vest_checks(IN _batch_from INT, IN _from INT, IN _to INT)
RETURNS TABLE (account INT, source_op BIGINT, effective_vests BIGINT)
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  _op_withdraw_vesting             INT := btracker_backend.op_withdraw_vesting();
  _op_fill_vesting_withdraw        INT := btracker_backend.op_fill_vesting_withdraw();
  _op_delegate_vesting_shares      INT := btracker_backend.op_delegate_vesting_shares();
  _op_return_vesting_delegation    INT := btracker_backend.op_return_vesting_delegation();
  _nai_vests                       INT := btracker_backend.nai_vests();
  _hf_vests_precision              INT := btracker_backend.hf_vests_precision();
  _hf_withdraw_rate                INT := btracker_backend.hf_withdraw_rate();
  _hf_withdraw_rate_round_up       INT := btracker_backend.hf_withdraw_rate_round_up();
  _hf_vests_precision_block        INT;
  _hf_withdraw_rate_block          INT;
  _hf_withdraw_rate_round_up_block INT;
BEGIN
SELECT
  MAX(CASE WHEN hardfork_num = _hf_vests_precision THEN block_num END),
  MAX(CASE WHEN hardfork_num = _hf_withdraw_rate THEN block_num END),
  MAX(CASE WHEN hardfork_num = _hf_withdraw_rate_round_up THEN block_num END)
INTO _hf_vests_precision_block, _hf_withdraw_rate_block, _hf_withdraw_rate_round_up_block
FROM hafd.applied_hardforks
WHERE hardfork_num IN (_hf_vests_precision, _hf_withdraw_rate, _hf_withdraw_rate_round_up);

RETURN QUERY
WITH vest_ops AS MATERIALIZED (
  SELECT ov.id, ov.block_num, ov.op_type_id, ov.body_value AS body
  FROM _btracker_ops_batch ov
  WHERE
    ov.op_type_id IN (_op_withdraw_vesting, _op_fill_vesting_withdraw, _op_delegate_vesting_shares, _op_return_vesting_delegation) AND
    ov.block_num BETWEEN _batch_from AND _to
),
withdraws AS MATERIALIZED (
  SELECT o.id AS source_op, o.block_num, w.account_name, w.vesting_withdraw_rate, w.to_withdraw
  FROM vest_ops o
  CROSS JOIN LATERAL btracker_backend.process_withdraw_vesting_operation(
    o.body,
    o.block_num > _hf_vests_precision_block,
    o.block_num > _hf_withdraw_rate_block,
    o.block_num > _hf_withdraw_rate_round_up_block
  ) w
  WHERE o.op_type_id = _op_withdraw_vesting
),
fills AS MATERIALIZED (
  SELECT o.id AS source_op, o.block_num, f.account_name, f.withdrawn
  FROM vest_ops o
  CROSS JOIN LATERAL btracker_backend.process_fill_vesting_withdraw_operation_for_withdrawals(o.body, o.block_num > _hf_vests_precision_block) f
  WHERE o.op_type_id = _op_fill_vesting_withdraw
),
delegations AS MATERIALIZED (
  SELECT o.id AS source_op, o.block_num, d.delegator, d.delegatee, d.amount
  FROM vest_ops o
  CROSS JOIN LATERAL btracker_backend.process_delegate_vesting_shares_operation(o.body) d
  WHERE o.op_type_id = _op_delegate_vesting_shares AND d.delegator <> d.delegatee
),
returns AS MATERIALIZED (
  SELECT o.id AS source_op, r.delegator AS account_name, r.amount
  FROM vest_ops o
  CROSS JOIN LATERAL btracker_backend.process_return_vesting_shares_operation(o.body) r
  WHERE o.op_type_id = _op_return_vesting_delegation
),
account_ids AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM accounts_view av
  WHERE av.name IN (
    SELECT account_name FROM withdraws
    UNION SELECT account_name FROM fills
    UNION SELECT delegator FROM delegations
    UNION SELECT delegatee FROM delegations
    UNION SELECT account_name FROM returns
  )
),
------------------------------------------------------------------------------
-- The checks hived makes in _from.._to, for every account
------------------------------------------------------------------------------
all_checks AS (
  SELECT ai.account_id AS account, w.source_op
  FROM withdraws w
  JOIN account_ids ai ON ai.account_name = w.account_name
  WHERE w.block_num >= _from

  UNION ALL

  SELECT ai.account_id, MAX(f.source_op)
  FROM fills f
  JOIN account_ids ai ON ai.account_name = f.account_name
  WHERE f.block_num >= _from
  GROUP BY ai.account_id, f.block_num

  UNION ALL

  SELECT ai.account_id, d.source_op
  FROM delegations d
  JOIN account_ids ai ON ai.account_name IN (d.delegator, d.delegatee)
  WHERE d.block_num >= _from
),
-- Only an account with RC delegations can overflow them.
rc_delegators AS MATERIALIZED (
  SELECT DISTINCT ac.account
  FROM all_checks ac
  WHERE
    EXISTS (SELECT 1 FROM current_rc_delegations crd WHERE crd.delegator = ac.account) OR
    EXISTS (SELECT 1 FROM _rc_delegation_ops rdo WHERE rdo.delegator = ac.account)
),
checks AS MATERIALIZED (
  SELECT DISTINCT ac.account, ac.source_op
  FROM all_checks ac
  JOIN rc_delegators rd ON rd.account = ac.account
),
------------------------------------------------------------------------------
-- The vesting events of those accounts since _batch_from
------------------------------------------------------------------------------
-- A payout is all fills of an account in one block; hived checks its VESTS once,
-- after the last of them.
payouts AS MATERIALIZED (
  SELECT ai.account_id AS account, MAX(f.source_op) AS source_op
  FROM fills f
  JOIN account_ids ai ON ai.account_name = f.account_name
  WHERE ai.account_id IN (SELECT rd.account FROM rc_delegators rd)
  GROUP BY ai.account_id, f.block_num
),
payout_vests_after AS (
  SELECT p.account, p.source_op, SUM(vb.balance) AS balance
  FROM payouts p
  JOIN _btracker_vests_batch vb ON vb.account_id = p.account AND vb.source_op > p.source_op
  GROUP BY p.account, p.source_op
),
-- The power-down ends at a payout that leaves the account with 0 VESTS.
payouts_leaving_no_vests AS MATERIALIZED (
  SELECT p.account, p.source_op
  FROM payouts p
  LEFT JOIN payout_vests_after pva ON pva.account = p.account AND pva.source_op = p.source_op
  LEFT JOIN current_account_balances cab ON cab.account = p.account AND cab.nai = _nai_vests
  WHERE COALESCE(cab.balance, 0) = COALESCE(pva.balance, 0)
),
delegation_pairs AS (
  SELECT dr.account_id AS delegator, de.account_id AS delegatee, d.amount, d.source_op
  FROM delegations d
  JOIN account_ids dr ON dr.account_name = d.delegator
  JOIN account_ids de ON de.account_name = d.delegatee
  WHERE
    dr.account_id IN (SELECT rd.account FROM rc_delegators rd) OR
    de.account_id IN (SELECT rd.account FROM rc_delegators rd)
),
-- As process_block_range_delegations: the delegatee's received vests follow every
-- change, the delegator's delegated vests only increases (a decrease comes back
-- with return_vesting_delegation).
delegation_deltas AS MATERIALIZED (
  SELECT
    dp.delegator,
    dp.delegatee,
    dp.source_op,
    dp.amount - COALESCE(
      LAG(dp.amount) OVER (PARTITION BY dp.delegator, dp.delegatee ORDER BY dp.source_op),
      cad.balance,
      0) AS delta
  FROM delegation_pairs dp
  LEFT JOIN current_accounts_delegations cad ON cad.delegator = dp.delegator AND cad.delegatee = dp.delegatee
),
events AS (
  SELECT ai.account_id AS account, w.source_op, TRUE AS starts_withdrawal, FALSE AS ends_withdrawal,
         w.vesting_withdraw_rate, w.to_withdraw, 0::BIGINT AS filled, 0::BIGINT AS delegated, 0::BIGINT AS received
  FROM withdraws w
  JOIN account_ids ai ON ai.account_name = w.account_name
  WHERE ai.account_id IN (SELECT rd.account FROM rc_delegators rd)

  UNION ALL

  SELECT ai.account_id, f.source_op, FALSE, pn.account IS NOT NULL, NULL, NULL, f.withdrawn, 0, 0
  FROM fills f
  JOIN account_ids ai ON ai.account_name = f.account_name
  LEFT JOIN payouts_leaving_no_vests pn ON pn.account = ai.account_id AND pn.source_op = f.source_op
  WHERE ai.account_id IN (SELECT rd.account FROM rc_delegators rd)

  UNION ALL

  SELECT dd.delegator, dd.source_op, FALSE, FALSE, NULL, NULL, 0, GREATEST(dd.delta, 0), 0
  FROM delegation_deltas dd
  WHERE dd.delegator IN (SELECT rd.account FROM rc_delegators rd)

  UNION ALL

  SELECT dd.delegatee, dd.source_op, FALSE, FALSE, NULL, NULL, 0, 0, dd.delta
  FROM delegation_deltas dd
  WHERE dd.delegatee IN (SELECT rd.account FROM rc_delegators rd)

  UNION ALL

  SELECT ai.account_id, r.source_op, FALSE, FALSE, NULL, NULL, 0, r.amount, 0
  FROM returns r
  JOIN account_ids ai ON ai.account_name = r.account_name
  WHERE ai.account_id IN (SELECT rd.account FROM rc_delegators rd)
),
-- withdrawal_no numbers the power-downs started in the batch: 0 is the one stored
-- in account_withdraws before it. withdrawal_ended: a payout of that power-down at
-- or before the event left 0 VESTS.
running_events AS (
  SELECT
    e.*,
    SUM(e.delegated) OVER w_account AS delegated_since,
    SUM(e.received) OVER w_account AS received_since,
    COUNT(*) FILTER (WHERE e.starts_withdrawal) OVER w_account AS withdrawal_no
  FROM events e
  WINDOW w_account AS (PARTITION BY e.account ORDER BY e.source_op)
),
event_states AS (
  SELECT
    re.account,
    re.source_op,
    re.delegated_since,
    re.received_since,
    re.withdrawal_no,
    FIRST_VALUE(re.vesting_withdraw_rate) OVER w_withdrawal AS vesting_withdraw_rate,
    FIRST_VALUE(re.to_withdraw) OVER w_withdrawal AS to_withdraw,
    SUM(re.filled) OVER w_withdrawal AS filled_since,
    BOOL_OR(re.ends_withdrawal) OVER w_withdrawal AS withdrawal_ended
  FROM running_events re
  WINDOW w_withdrawal AS (PARTITION BY re.account, re.withdrawal_no ORDER BY re.source_op)
),
vests_after AS (
  SELECT c.account, c.source_op, SUM(vb.balance) AS balance
  FROM checks c
  JOIN _btracker_vests_batch vb ON vb.account_id = c.account AND vb.source_op > c.source_op
  GROUP BY c.account, c.source_op
),
check_states AS (
  SELECT
    c.account,
    c.source_op,
    COALESCE(cab.balance, 0) - COALESCE(va.balance, 0) AS vests,
    COALESCE(ad.delegated_vests, 0) + COALESCE(es.delegated_since, 0) AS delegated_vests,
    COALESCE(ad.received_vests, 0) + COALESCE(es.received_since, 0) AS received_vests,
    CASE WHEN es.withdrawal_ended THEN 0 WHEN es.withdrawal_no > 0 THEN es.vesting_withdraw_rate ELSE COALESCE(aw.vesting_withdraw_rate, 0) END AS vesting_withdraw_rate,
    CASE WHEN es.withdrawal_ended THEN 0 WHEN es.withdrawal_no > 0 THEN es.to_withdraw ELSE COALESCE(aw.to_withdraw, 0) END AS to_withdraw,
    CASE WHEN es.withdrawal_ended THEN 0
         ELSE CASE WHEN es.withdrawal_no > 0 THEN 0 ELSE COALESCE(aw.withdrawn, 0) END + COALESCE(es.filled_since, 0) END AS withdrawn
  FROM checks c
  JOIN event_states es ON es.account = c.account AND es.source_op = c.source_op
  LEFT JOIN vests_after va ON va.account = c.account AND va.source_op = c.source_op
  LEFT JOIN current_account_balances cab ON cab.account = c.account AND cab.nai = _nai_vests
  LEFT JOIN account_delegations ad ON ad.account = c.account
  LEFT JOIN account_withdraws aw ON aw.account = c.account
)
SELECT
  cs.account,
  cs.source_op,
  (cs.vests - cs.delegated_vests + cs.received_vests
   - CASE WHEN cs.to_withdraw > cs.withdrawn THEN LEAST(cs.vesting_withdraw_rate, cs.to_withdraw - cs.withdrawn) ELSE 0 END)::BIGINT
FROM check_states cs;
END
$$;

/**
 * remove_rc_delegation_overflow(_delegators, _effective_vests, _source_ops)
 * -------------------------------------------------------------------------
 * hived's resource_credits::remove_delegations for each delegator whose RC
 * delegations exceed its effective vests: the overflow (sum of its delegations
 * minus _effective_vests) is taken from its delegations in delegatee account id
 * order, each reduced by what remains of the overflow and removed when that
 * consumes it, and account_rc_delegations follows on both sides. The arrays are
 * parallel; a delegator must appear at most once. A reduced delegation's
 * source_op becomes the delegator's _source_ops entry, or stays when it is NULL.
 *
 * hived removes at most get_remove_threshold() delegations per block and finishes
 * the rest in later blocks (rc_expired_delegation_object); this applies the whole
 * reduction at once, which leaves the same delegations.
 *
 * @returns number of delegations reduced or removed
 */
CREATE OR REPLACE FUNCTION remove_rc_delegation_overflow(IN _delegators INT[], IN _effective_vests BIGINT[], IN _source_ops BIGINT[])
RETURNS INT
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __delegators INT[];
  __delegatees INT[];
  __cuts       BIGINT[];
  __removes    BOOLEAN[];
  __source_ops BIGINT[];
BEGIN
SELECT array_agg(r.delegator), array_agg(r.delegatee), array_agg(r.cut), array_agg(r.cut = r.max_rc), array_agg(r.source_op)
INTO __delegators, __delegatees, __cuts, __removes, __source_ops
FROM (
  SELECT
    crd.delegator,
    crd.delegatee,
    crd.max_rc,
    -- what is left of the overflow once the delegations before this one took theirs
    LEAST(crd.max_rc, GREATEST(
      SUM(crd.max_rc) OVER w_all - c.effective_vests - (SUM(crd.max_rc) OVER w_before - crd.max_rc),
      0)) AS cut,
    c.source_op
  FROM unnest(_delegators, _effective_vests, _source_ops) AS c(delegator, effective_vests, source_op)
  JOIN current_rc_delegations crd ON crd.delegator = c.delegator
  WINDOW
    w_all AS (PARTITION BY crd.delegator),
    w_before AS (PARTITION BY crd.delegator ORDER BY crd.delegatee)
) r
WHERE r.cut > 0;

-- HAF's statement-level triggers reject edits of registered tables in some states
-- (see db/one_off_repairs.sql): issue none when there is nothing to change.
IF __delegators IS NULL THEN
  RETURN 0;
END IF;

UPDATE current_rc_delegations crd
SET max_rc = crd.max_rc - c.cut, source_op = COALESCE(c.source_op, crd.source_op)
FROM unnest(__delegators, __delegatees, __cuts, __removes, __source_ops) AS c(delegator, delegatee, cut, removes, source_op)
WHERE crd.delegator = c.delegator AND crd.delegatee = c.delegatee AND NOT c.removes;

DELETE FROM current_rc_delegations crd
USING unnest(__delegators, __delegatees, __removes) AS c(delegator, delegatee, removes)
WHERE crd.delegator = c.delegator AND crd.delegatee = c.delegatee AND c.removes;

INSERT INTO account_rc_delegations AS ard (account, received_rc, delegated_rc)
SELECT s.account, SUM(s.received_rc), SUM(s.delegated_rc)
FROM (
  SELECT c.delegatee AS account, -c.cut AS received_rc, 0 AS delegated_rc
  FROM unnest(__delegatees, __cuts) AS c(delegatee, cut)
  UNION ALL
  SELECT c.delegator, 0, -c.cut
  FROM unnest(__delegators, __cuts) AS c(delegator, cut)
) s
GROUP BY s.account
ON CONFLICT ON CONSTRAINT pk_account_rc_delegations
DO UPDATE SET
  received_rc = ard.received_rc + EXCLUDED.received_rc,
  delegated_rc = ard.delegated_rc + EXCLUDED.delegated_rc;

RETURN cardinality(__delegators);
END
$$;

/**
 * set_rc_delegation(_delegator, _delegatee, _max_rc, _source_op)
 * ---------------------------------------------------------------
 * Applies one delegate_rc to one delegatee: sets the pair to _max_rc (removes it
 * at 0) and moves account_rc_delegations of both sides by the change.
 */
CREATE OR REPLACE FUNCTION set_rc_delegation(IN _delegator INT, IN _delegatee INT, IN _max_rc BIGINT, IN _source_op BIGINT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
AS
$$
DECLARE
  __previous BIGINT;
BEGIN
SELECT crd.max_rc INTO __previous
FROM current_rc_delegations crd
WHERE crd.delegator = _delegator AND crd.delegatee = _delegatee;

IF _max_rc > 0 THEN
  INSERT INTO current_rc_delegations AS crd (delegator, delegatee, max_rc, source_op)
  VALUES (_delegator, _delegatee, _max_rc, _source_op)
  ON CONFLICT ON CONSTRAINT pk_current_rc_delegations
  DO UPDATE SET max_rc = EXCLUDED.max_rc, source_op = EXCLUDED.source_op;
ELSIF __previous IS NOT NULL THEN
  DELETE FROM current_rc_delegations crd
  WHERE crd.delegator = _delegator AND crd.delegatee = _delegatee;
END IF;

INSERT INTO account_rc_delegations AS ard (account, received_rc, delegated_rc)
VALUES
  (_delegator, 0, _max_rc - COALESCE(__previous, 0)),
  (_delegatee, _max_rc - COALESCE(__previous, 0), 0)
ON CONFLICT ON CONSTRAINT pk_account_rc_delegations
DO UPDATE SET
  received_rc = ard.received_rc + EXCLUDED.received_rc,
  delegated_rc = ard.delegated_rc + EXCLUDED.delegated_rc;
END
$$;

/**
 * process_rc_delegation_range(_batch_from, _from, _to)
 * ----------------------------------------------------
 * The RC delegation changes of blocks _from.._to (all at or after HF26) of the
 * batch that starts at _batch_from.
 *
 * An account that hived checks for RC delegation overflow in the range
 * (rc_delegation_vest_checks) has its delegate_rc ops and those checks applied one
 * by one in operation order, so a reduction lands at the operation that caused it
 * and a later delegate_rc acts on the reduced delegations. Every other delegator's
 * delegate_rc ops are squashed set-based (STEP 5 on).
 */
CREATE OR REPLACE FUNCTION process_rc_delegation_range(IN _batch_from INT, IN _from INT, IN _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET from_collapse_limit = 16
SET join_collapse_limit = 16
SET jit = OFF
AS
$$
DECLARE
  -- Operation type IDs (looked up dynamically from hafd.operation_types)
  _op_custom_json             INT := btracker_backend.op_custom_json();
  _op_withdraw_vesting        INT := btracker_backend.op_withdraw_vesting();
  _op_fill_vesting_withdraw   INT := btracker_backend.op_fill_vesting_withdraw();
  _op_delegate_vesting_shares INT := btracker_backend.op_delegate_vesting_shares();
  __vest_check_names TEXT[];
  __event RECORD;
  -- Counters
  __insert_current_rc_delegations INT;
  __delete_canceled_rc_delegations INT;
  __insert_rc_delegations INT;
BEGIN
------------------------------------------------------------------------------
-- Nearly every block has no delegate_rc op and no vest check of an account
-- with RC delegations. Then _rc_delegation_ops is empty, so the checks are
-- limited to current RC delegators (rc_delegation_vest_checks.rc_delegators)
-- and there are none either: nothing below would change.
--
-- The account lookup takes the names as a parameter rather than reading
-- _btracker_ops_batch, so its plan survives a recreation of that temp table
-- (every massive-sync batch).
------------------------------------------------------------------------------
IF NOT EXISTS (
  SELECT 1
  FROM _btracker_ops_batch ov
  WHERE
    ov.op_type_id = _op_custom_json AND
    ov.block_num BETWEEN _from AND _to AND
    (ov.body_value->>'id') = 'rc'
) THEN
  SELECT array_agg(DISTINCT n.account_name)
  INTO __vest_check_names
  FROM _btracker_ops_batch ov
  CROSS JOIN LATERAL (VALUES
    (ov.body_value->>'account'),
    (ov.body_value->>'from_account'),
    (ov.body_value->>'delegator'),
    (ov.body_value->>'delegatee')
  ) AS n(account_name)
  WHERE
    ov.op_type_id IN (_op_withdraw_vesting, _op_fill_vesting_withdraw, _op_delegate_vesting_shares) AND
    ov.block_num BETWEEN _from AND _to AND
    n.account_name IS NOT NULL;

  IF NOT EXISTS (
    SELECT 1
    FROM hive.accounts_view av
    JOIN current_rc_delegations crd ON crd.delegator = av.id
    WHERE av.name = ANY(__vest_check_names)
  ) THEN
    RETURN;
  END IF;
END IF;

------------------------------------------------------------------------------
-- STEPS 1-4: the range's delegate_rc ops, one row per delegatee
--
-- Fetch the custom_json operations with id='rc', parse them with the C++ parser
-- hive.parse_rc_delegation (one row per delegatee; NULL for an invalid op) and
-- resolve the account names to IDs.
------------------------------------------------------------------------------
-- Both temp tables are kept across single-block batches (live), so the plans
-- that read them stay cached (btracker_reset_batch_table).
IF NOT btracker_reset_batch_table('_rc_delegation_ops', _batch_from = _to) THEN
  CREATE TEMP TABLE _rc_delegation_ops(
    delegator       INT,
    delegatee       INT,
    max_rc          BIGINT,
    source_op       BIGINT,
    source_op_block INT
  );
END IF;

INSERT INTO _rc_delegation_ops
WITH ops AS MATERIALIZED (
  SELECT ov.body_value AS body, ov.id, ov.block_num
  FROM _btracker_ops_batch ov
  WHERE
    ov.op_type_id = _op_custom_json AND
    ov.block_num BETWEEN _from AND _to AND
    (ov.body_value->>'id') = 'rc'
),
parsed_rc_delegations AS MATERIALIZED (
  SELECT
    parsed.from_account,
    parsed.to_account,
    parsed.max_rc,
    o.id AS source_op,
    o.block_num AS source_op_block
  FROM ops o
  CROSS JOIN LATERAL hive.parse_rc_delegation((o.body->>'json')::text) AS parsed
  WHERE parsed.from_account IS NOT NULL
),
all_account_names AS (
  SELECT DISTINCT from_account AS account_name FROM parsed_rc_delegations
  UNION
  SELECT DISTINCT to_account AS account_name FROM parsed_rc_delegations
),
account_ids AS MATERIALIZED (
  SELECT av.name AS account_name, av.id AS account_id
  FROM hive.accounts_view av
  WHERE av.name IN (SELECT account_name FROM all_account_names)
)
SELECT
  delegator.account_id AS delegator,
  delegatee.account_id AS delegatee,
  p.max_rc,
  p.source_op,
  p.source_op_block
FROM parsed_rc_delegations p
JOIN account_ids delegator ON delegator.account_name = p.from_account
JOIN account_ids delegatee ON delegatee.account_name = p.to_account;

IF NOT btracker_reset_batch_table('_rc_vest_checks', _batch_from = _to) THEN
  CREATE TEMP TABLE _rc_vest_checks(
    account         INT,
    source_op       BIGINT,
    effective_vests BIGINT
  );
END IF;

INSERT INTO _rc_vest_checks
SELECT * FROM rc_delegation_vest_checks(_batch_from, _from, _to);

------------------------------------------------------------------------------
-- Accounts hived checks for overflow in the range: their delegate_rc ops and the
-- checks, in operation order.
------------------------------------------------------------------------------
FOR __event IN
  SELECT FALSE AS is_check, rdo.delegator AS account, rdo.delegatee, rdo.max_rc, NULL::BIGINT AS effective_vests, rdo.source_op
  FROM _rc_delegation_ops rdo
  WHERE rdo.delegator IN (SELECT rvc.account FROM _rc_vest_checks rvc)

  UNION ALL

  SELECT TRUE, rvc.account, NULL, NULL, rvc.effective_vests, rvc.source_op
  FROM _rc_vest_checks rvc

  ORDER BY source_op
LOOP
  IF __event.is_check THEN
    PERFORM remove_rc_delegation_overflow(ARRAY[__event.account], ARRAY[__event.effective_vests], ARRAY[__event.source_op]);
  ELSE
    PERFORM set_rc_delegation(__event.account, __event.delegatee, __event.max_rc, __event.source_op);
  END IF;
END LOOP;

------------------------------------------------------------------------------
-- Every other delegator: squash its delegate_rc ops.
------------------------------------------------------------------------------
WITH rc_delegations_with_ids AS MATERIALIZED (
  SELECT
    rdo.delegator,
    rdo.delegatee,
    rdo.max_rc,
    rdo.source_op,
    rdo.source_op_block
  FROM _rc_delegation_ops rdo
  WHERE rdo.delegator NOT IN (SELECT rvc.account FROM _rc_vest_checks rvc)
),
------------------------------------------------------------------------------
-- STEP 5: Get previous RC delegation balances for delta calculation
--
-- THE SQUASHING PATTERN:
-- We process many operations but only store the FINAL STATE.
-- To calculate correct deltas, we need the STARTING balance.
------------------------------------------------------------------------------
group_by_delegator_delegatee AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    MAX(source_op) as source_op
  FROM rc_delegations_with_ids
  GROUP BY delegator, delegatee
),
------------------------------------------------------------------------------
-- Fetch previous RC delegation balance from current_rc_delegations table.
-- If no previous delegation exists (new delegation pair), default to 0.
-- source_op=0 marks this as a "synthetic" previous record.
------------------------------------------------------------------------------
get_prev_rc_delegation AS (
  SELECT
    gb.delegator,
    gb.delegatee,
    COALESCE(crd.max_rc, 0) as max_rc,
    0 AS source_op,
    0 AS source_op_block
  FROM group_by_delegator_delegatee gb
  LEFT JOIN current_rc_delegations crd ON crd.delegator = gb.delegator AND crd.delegatee = gb.delegatee
),
------------------------------------------------------------------------------
-- Combine current operations with previous balances.
-- This creates a complete timeline for each (delegator, delegatee) pair.
------------------------------------------------------------------------------
add_prev_rc_delegation AS (
  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op,
    source_op_block
  FROM rc_delegations_with_ids

  UNION ALL

  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op,
    source_op_block
  FROM get_prev_rc_delegation
),
------------------------------------------------------------------------------
-- STEP 6: Calculate deltas using window functions
--
-- LAG(max_rc, 1, 0): Get previous row's max_rc (default 0 for first row)
-- rc_delta = current_max_rc - previous_max_rc
--
-- LAST OPERATION WINS PATTERN:
-- ROW_NUMBER() OVER (ORDER BY source_op DESC) assigns rn=1 to the LATEST operation.
-- We only persist the final state (rn=1) to current_rc_delegations.
------------------------------------------------------------------------------
rc_delegation_delta AS MATERIALIZED (
  SELECT
    delegator,
    delegatee,
    max_rc,
    max_rc - LAG(max_rc, 1, 0) OVER w_asc AS rc_delta,
    source_op,
    source_op_block,
    ROW_NUMBER() OVER w_desc AS rn  -- rn=1 is the LATEST operation
  FROM add_prev_rc_delegation
  WINDOW
    w_asc AS (PARTITION BY delegator, delegatee ORDER BY source_op),
    w_desc AS (PARTITION BY delegator, delegatee ORDER BY source_op DESC)
),
------------------------------------------------------------------------------
-- STEP 7: Calculate aggregated RC changes per account
--
-- When alice delegates 1000 RC to bob:
--   - Alice's delegated_rc goes up by 1000
--   - Bob's received_rc goes up by 1000
--
-- The account_rc_delegations table stores CUMULATIVE totals.
------------------------------------------------------------------------------
union_rc_delegations AS (
  -- Delegator side (RC going OUT)
  SELECT
    dd.delegator AS account_id,
    0 AS received_rc,
    dd.rc_delta AS delegated_rc
  FROM rc_delegation_delta dd
  WHERE dd.source_op > 0  -- Exclude synthetic previous record

  UNION ALL

  -- Delegatee side (RC coming IN)
  SELECT
    delegatee AS account_id,
    rc_delta AS received_rc,
    0
  FROM rc_delegation_delta
  WHERE source_op > 0  -- Exclude synthetic previous record
),
------------------------------------------------------------------------------
-- Aggregate all RC delegation changes per account
------------------------------------------------------------------------------
sum_rc_delegations AS (
  SELECT
    ud.account_id,
    SUM(ud.received_rc) AS received_rc,
    SUM(ud.delegated_rc) AS delegated_rc
  FROM union_rc_delegations ud
  GROUP BY ud.account_id
),
------------------------------------------------------------------------------
-- STEP 8: Extract FINAL RC delegation state for each (delegator, delegatee) pair
--
-- SQUASHING RESULT: From all the operations processed, we only need the
-- LATEST state (rn=1) to store in current_rc_delegations.
------------------------------------------------------------------------------
prepare_newest_rc_delegation_pairs AS MATERIALIZED (
  SELECT
    dd.delegator,
    dd.delegatee,
    dd.max_rc,
    dd.source_op,
    dd.source_op_block
  FROM rc_delegation_delta dd
  WHERE dd.rn = 1 AND dd.source_op > 0
),
------------------------------------------------------------------------------
-- STEP 9: Persist final RC delegation states (UPSERT active delegations)
--
-- Only INSERT/UPDATE where max_rc > 0 (active delegations).
-- Delegations with max_rc = 0 are handled in delete_canceled_rc_delegations.
------------------------------------------------------------------------------
insert_current_rc_delegations AS (
  INSERT INTO current_rc_delegations AS crd
    (delegator, delegatee, max_rc, source_op)
  SELECT
    delegator,
    delegatee,
    max_rc,
    source_op
  FROM prepare_newest_rc_delegation_pairs
  WHERE max_rc > 0  -- Only active delegations
  ON CONFLICT ON CONSTRAINT pk_current_rc_delegations
  DO UPDATE SET
      max_rc = EXCLUDED.max_rc,
      source_op = EXCLUDED.source_op
  RETURNING crd.delegator AS delegator
),
------------------------------------------------------------------------------
-- STEP 10: Delete canceled RC delegations (max_rc = 0)
--
-- When an RC delegation is removed (max_rc set to 0), we DELETE the row from
-- current_rc_delegations rather than keeping a zero-balance record.
------------------------------------------------------------------------------
delete_canceled_rc_delegations AS (
  DELETE FROM current_rc_delegations crd
  USING prepare_newest_rc_delegation_pairs pn
  WHERE
    crd.delegator = pn.delegator AND
    crd.delegatee = pn.delegatee AND pn.max_rc = 0
  RETURNING crd.delegator AS delegator
),
------------------------------------------------------------------------------
-- STEP 11: Update account_rc_delegations summary table (UPSERT)
--
-- account_rc_delegations stores CUMULATIVE totals per account:
--   - received_rc: Total RC received from all delegators
--   - delegated_rc: Total RC delegated out to all delegatees
------------------------------------------------------------------------------
insert_rc_delegations AS (
  INSERT INTO account_rc_delegations
    (account, received_rc, delegated_rc)
  SELECT
    sd.account_id,
    sd.received_rc,
    sd.delegated_rc
  FROM sum_rc_delegations sd
  ON CONFLICT ON CONSTRAINT pk_account_rc_delegations
  DO UPDATE SET
      received_rc = account_rc_delegations.received_rc + EXCLUDED.received_rc,
      delegated_rc = account_rc_delegations.delegated_rc + EXCLUDED.delegated_rc
  RETURNING account AS updated_account
)

------------------------------------------------------------------------------
-- STEP 12: Return counts for logging/debugging
--
-- Execute all CTEs and capture counts from data-modifying CTEs.
------------------------------------------------------------------------------
SELECT
  (SELECT count(*) FROM insert_current_rc_delegations) AS insert_current_rc_delegations,
  (SELECT count(*) FROM delete_canceled_rc_delegations) AS delete_canceled_rc_delegations,
  (SELECT count(*) FROM insert_rc_delegations) AS insert_rc_delegations
INTO __insert_current_rc_delegations, __delete_canceled_rc_delegations, __insert_rc_delegations;

END
$$;

RESET ROLE;
