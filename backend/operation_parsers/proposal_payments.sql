/*
Operation parser for DHF proposal payments.
Called by: db/process_proposal_payments.sql

proposal_pay_operation (virtual) is emitted once per paid proposal at every DHF
maintenance period: {proposal_id, receiver, payer, payment}. HAF stores the body
hived's replay produces, so every one carries proposal_id, including the payments
made before HF24 (payer steem.dao). The treasury pays in HBD only.
*/

SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.proposal_payment_event CASCADE;
CREATE TYPE btracker_backend.proposal_payment_event AS
(
    proposal_id INT,
    receiver    TEXT,
    amount      BIGINT  -- HBD satoshis (×1000)
);

CREATE OR REPLACE FUNCTION btracker_backend.get_proposal_payment_event(IN _body JSONB)
RETURNS btracker_backend.proposal_payment_event
LANGUAGE plpgsql IMMUTABLE
AS
$$
BEGIN
  RETURN (
    (_body ->> 'proposal_id')::INT,
    _body ->> 'receiver',
    (_body -> 'payment' ->> 'amount')::BIGINT
  )::btracker_backend.proposal_payment_event;
END;
$$;

RESET ROLE;
