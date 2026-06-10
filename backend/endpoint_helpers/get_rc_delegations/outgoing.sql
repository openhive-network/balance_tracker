SET ROLE btracker_owner;

-- Outgoing RC delegations reader for the /rc-delegations endpoint.

/*
Returns RC delegations sent to other accounts.
Called by: btracker_endpoints.get_rc_delegations()
*/
CREATE OR REPLACE FUNCTION btracker_backend.outgoing_rc_delegations(IN _account_id INT)
RETURNS SETOF btracker_backend.outgoing_rc_delegations
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.delegatee)::TEXT AS delegatee,
      d.max_rc::TEXT AS max_rc,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num
    FROM btracker_backend.current_rc_delegations_view d
    WHERE d.delegator = _account_id;
END
$$;

RESET ROLE;
