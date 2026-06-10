SET ROLE btracker_owner;

-- Outgoing VESTS delegations reader for the /delegations endpoint.

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
