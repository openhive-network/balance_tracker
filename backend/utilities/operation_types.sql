SET ROLE btracker_owner;

-- Convenience functions for commonly used operation types
-- These provide semantic names and avoid string literals scattered throughout the code
-- Each function returns the operation type ID by looking up the name in hafd.operation_types

-- Transfer operations
CREATE OR REPLACE FUNCTION btracker_backend.op_transfer()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::transfer_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_transfer_to_vesting()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::transfer_to_vesting_operation');
END;
$$;

-- Withdrawal operations
CREATE OR REPLACE FUNCTION btracker_backend.op_withdraw_vesting()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::withdraw_vesting_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_set_withdraw_vesting_route()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::set_withdraw_vesting_route_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_vesting_withdraw()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_vesting_withdraw_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_transfer_to_vesting_completed()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::transfer_to_vesting_completed_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_delayed_voting()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::delayed_voting_operation');
END;
$$;

-- Delegation operations
CREATE OR REPLACE FUNCTION btracker_backend.op_delegate_vesting_shares()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::delegate_vesting_shares_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_account_create_with_delegation()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::account_create_with_delegation_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_return_vesting_delegation()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::return_vesting_delegation_operation');
END;
$$;

-- Savings operations
CREATE OR REPLACE FUNCTION btracker_backend.op_transfer_to_savings()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::transfer_to_savings_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_transfer_from_savings()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::transfer_from_savings_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_cancel_transfer_from_savings()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::cancel_transfer_from_savings_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_transfer_from_savings()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_transfer_from_savings_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_interest()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::interest_operation');
END;
$$;

-- Reward operations
CREATE OR REPLACE FUNCTION btracker_backend.op_claim_reward_balance()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::claim_reward_balance_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_author_reward()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::author_reward_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_curation_reward()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::curation_reward_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_comment_reward()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::comment_reward_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_comment_benefactor_reward()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::comment_benefactor_reward_operation');
END;
$$;

-- Recurrent transfer operations
CREATE OR REPLACE FUNCTION btracker_backend.op_recurrent_transfer()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::recurrent_transfer_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_recurrent_transfer()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_recurrent_transfer_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_failed_recurrent_transfer()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::failed_recurrent_transfer_operation');
END;
$$;

-- Escrow operations
CREATE OR REPLACE FUNCTION btracker_backend.op_escrow_transfer()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_transfer_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_escrow_dispute()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_dispute_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_escrow_release()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_release_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_escrow_approved()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_approved_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_escrow_rejected()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::escrow_rejected_operation');
END;
$$;

-- Hardfork operations
CREATE OR REPLACE FUNCTION btracker_backend.op_hardfork_hive()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::hardfork_hive_operation');
END;
$$;

-- Limit order operations
CREATE OR REPLACE FUNCTION btracker_backend.op_limit_order_create()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::limit_order_create_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_limit_order_create2()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::limit_order_create2_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_limit_order_cancel()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::limit_order_cancel_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_limit_order_cancelled()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::limit_order_cancelled_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_order()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_order_operation');
END;
$$;

-- Convert operations
CREATE OR REPLACE FUNCTION btracker_backend.op_convert()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::convert_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_convert_request()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_convert_request_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_collateralized_convert()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::collateralized_convert_operation');
END;
$$;

CREATE OR REPLACE FUNCTION btracker_backend.op_fill_collateralized_convert_request()
RETURNS INT LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN (SELECT id FROM hafd.operation_types WHERE name = 'hive::protocol::fill_collateralized_convert_request_operation');
END;
$$;

RESET ROLE;
