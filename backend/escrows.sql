SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.escrow_event CASCADE;
CREATE TYPE btracker_backend.escrow_event AS
(
    from_name   TEXT,
    to_name     TEXT,
    agent_name  TEXT,
    escrow_id   BIGINT,
    nai         SMALLINT,   -- 13=HBD, 21=HIVE (NULL for non-amount events)
    amount      BIGINT,     -- millis; NULL for non-amount events
    kind        TEXT,       -- 'transfer' | 'release' | 'approved' | 'rejected' | 'dispute' | 'approve_intent'
    fee         BIGINT,     -- only used for 'approved' if virtual op carries it
    fee_nai     SMALLINT    -- 13 or 21 for fee (if present)
);

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_events(
    IN _body jsonb,
    IN _op_name text
) RETURNS SETOF btracker_backend.escrow_event
LANGUAGE plpgsql STABLE AS
$$
DECLARE
    _from   text;
    _to     text;
    _agent  text;
    _id     bigint;

    _hbd_amt  bigint; _hbd_nai  smallint;
    _hive_amt bigint; _hive_nai smallint;

    _fee_amt bigint; _fee_nai smallint;
BEGIN
  -- Common keys (ignore 'who'/'receiver')
    _from  := (_body->'value'->>'from')::text;
    _to    := (_body->'value'->>'to')::text;
    _agent := (_body->'value'->>'agent')::text;
    _id    := (_body->'value'->>'escrow_id')::bigint;

  IF _op_name = 'hive::protocol::escrow_transfer_operation' THEN
    _hbd_amt  := NULLIF((_body->'value'->'hbd_amount' ->>'amount'), '')::bigint;
    _hbd_nai  := NULLIF(substring((_body->'value'->'hbd_amount' ->>'nai') FROM 3),'')::smallint;
    _hive_amt := NULLIF((_body->'value'->'hive_amount'->>'amount'), '')::bigint;
    _hive_nai := NULLIF(substring((_body->'value'->'hive_amount'->>'nai') FROM 3),'')::smallint;

    _fee_amt  := NULLIF((_body->'value'->'fee'->>'amount'), '')::bigint;
    _fee_nai  := NULLIF(substring((_body->'value'->'fee'->>'nai') FROM 3),'')::smallint;

    IF COALESCE(_hbd_amt,0) > 0 THEN
      RETURN QUERY SELECT _from, _to, _agent, _id, _hbd_nai,  _hbd_amt,  'transfer', _fee_amt, _fee_nai;
    END IF;
    IF COALESCE(_hive_amt,0) > 0 THEN
      RETURN QUERY SELECT _from, _to, _agent, _id, _hive_nai, _hive_amt, 'transfer', _fee_amt, _fee_nai;
    END IF;

  ELSIF _op_name = 'hive::protocol::escrow_release_operation' THEN
    _hbd_amt  := NULLIF((_body->'value'->'hbd_amount' ->>'amount'), '')::bigint;
    _hbd_nai  := NULLIF(substring((_body->'value'->'hbd_amount' ->>'nai') FROM 3),'')::smallint;
    _hive_amt := NULLIF((_body->'value'->'hive_amount'->>'amount'), '')::bigint;
    _hive_nai := NULLIF(substring((_body->'value'->'hive_amount'->>'nai') FROM 3),'')::smallint;

    IF COALESCE(_hbd_amt,0) > 0 THEN
      RETURN QUERY SELECT _from, _to, _agent, _id, _hbd_nai,  _hbd_amt,  'release', NULL::bigint, NULL::smallint;
    END IF;
    IF COALESCE(_hive_amt,0) > 0 THEN
      RETURN QUERY SELECT _from, _to, _agent, _id, _hive_nai, _hive_amt, 'release', NULL::bigint, NULL::smallint;
    END IF;

  ELSIF _op_name = 'hive::protocol::escrow_approve_operation' THEN
    -- Intent only, for audit
    RETURN QUERY SELECT _from, _to, _agent, _id, NULL::smallint, NULL::bigint, 'approve_intent', NULL::bigint, NULL::smallint;

  ELSIF _op_name = 'hive::protocol::escrow_approved_operation' THEN
    -- Virtual approval; carry fee if present (regardless of 'who')
    _fee_amt := NULLIF((_body->'value'->'fee'->>'amount'), '')::bigint;
    _fee_nai := NULLIF(substring((_body->'value'->'fee'->>'nai') FROM 3),'')::smallint;
    RETURN QUERY SELECT _from, _to, _agent, _id, NULL::smallint, NULL::bigint, 'approved', _fee_amt, _fee_nai;

  ELSIF _op_name = 'hive::protocol::escrow_rejected_operation' THEN
    -- Generic rejection; ignore 'who'
    RETURN QUERY SELECT _from, _to, _agent, _id, NULL::smallint, NULL::bigint, 'rejected', NULL::bigint, NULL::smallint;

  ELSIF _op_name = 'hive::protocol::escrow_dispute_operation' THEN
    RETURN QUERY SELECT _from, _to, _agent, _id, NULL::smallint, NULL::bigint, 'dispute', NULL::bigint, NULL::smallint;
  END IF;

  RETURN;
END;
$$;

RESET ROLE;
