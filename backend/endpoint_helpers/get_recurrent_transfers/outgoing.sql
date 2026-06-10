SET ROLE btracker_owner;

-- Outgoing recurrent transfers reader for the /recurrent-transfers endpoint.

/*
Returns scheduled recurring transfers FROM this account.
Called by: btracker_endpoints.get_recurrent_transfers()

consecutive_failures: Incremented on insufficient funds, auto-cancelled after 10
remaining_executions: Decremented on each successful execution
*/
CREATE OR REPLACE FUNCTION btracker_backend.outgoing_recurrent_transfers(IN _account_id INT)
RETURNS SETOF btracker_backend.outgoing_recurrent_transfers
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN QUERY
    SELECT
      (SELECT av.name FROM hive.accounts_view av WHERE av.id = d.to_account)::TEXT AS to,
      d.transfer_id AS pair_id,
      btracker_backend.create_amount_object(d.nai, d.amount) AS amount,
      d.consecutive_failures AS consecutive_failures,
      d.remaining_executions AS remaining_executions,
      d.recurrence AS recurrence,
      d.memo AS memo,
      bv.created_at + (d.recurrence::TEXT || 'hour')::INTERVAL AS trigger_date,
      d.source_op::TEXT AS operation_id,
      d.source_op_block AS block_num
    FROM btracker_backend.recurrent_transfers_view d
    JOIN hive.blocks_view bv ON bv.num = d.source_op_block
    WHERE d.from_account = _account_id;
END
$$;

RESET ROLE;
