SET ROLE btracker_owner;

/*
Returns VESTS delegations received from other accounts.
Called by: btracker_endpoints.get_balance_delegations()
Uses idx_current_accounts_delegations_delegatee_idx (created after massive sync).
*/
CREATE OR REPLACE FUNCTION btracker_backend.incoming_delegations(IN _account_id INT)
RETURNS SETOF btracker_backend.incoming_delegations
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      -- Scalar subquery for name lookup - efficient for typical delegation counts
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegator)::TEXT AS delegator,
      d.balance::TEXT AS amount,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num
    FROM btracker_backend.current_accounts_delegations_view d
    WHERE d.delegatee = _account_id;
END
$$;

/*
Returns VESTS delegations sent to other accounts.
Called by: btracker_endpoints.get_balance_delegations()
*/
CREATE OR REPLACE FUNCTION btracker_backend.outgoing_delegations(IN _account_id INT)
RETURNS SETOF btracker_backend.outgoing_delegations
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegatee)::TEXT AS delegatee,
      d.balance::TEXT AS amount,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num
    FROM btracker_backend.current_accounts_delegations_view d
    WHERE d.delegator = _account_id;
END
$$;

RESET ROLE;
