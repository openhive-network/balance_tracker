SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.escrow_event_agg CASCADE;
CREATE TYPE btracker_backend.escrow_event_agg AS (
    from_name    text,
    to_name      text,
    agent_name   text,
    escrow_id    bigint,
    kind         text,        -- 'transfer' | 'release' | 'approved' | 'rejected'
    hbd_amount   bigint,      -- millis (transfer/release)
    hive_amount  bigint,      -- millis (transfer/release)
    fee_amount   bigint,      -- millis (transfer: agent fee, approved: approval fee)
    fee_nai      smallint     -- 13 or 21 for the fee (if present)
);

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_event_agg(
    _body jsonb,
    _op_name text
) RETURNS btracker_backend.escrow_event_agg
LANGUAGE plpgsql STABLE AS
$$
DECLARE
    r btracker_backend.escrow_event_agg;
BEGIN
    r.from_name  := (_body->'value'->>'from')::text;
    r.to_name    := (_body->'value'->>'to')::text;
    r.agent_name := (_body->'value'->>'agent')::text;
    r.escrow_id  := (_body->'value'->>'escrow_id')::bigint;

    IF _op_name = 'hive::protocol::escrow_transfer_operation' THEN
        r.kind := 'transfer';
        r.hbd_amount  := NULLIF((_body->'value'->'hbd_amount' ->>'amount'), '')::bigint;
        r.hive_amount := NULLIF((_body->'value'->'hive_amount'->>'amount'), '')::bigint;
        r.fee_amount  := NULLIF((_body->'value'->'fee'->>'amount'), '')::bigint;
        r.fee_nai     := NULLIF(substring((_body->'value'->'fee'->>'nai') FROM 3), '')::smallint;

    ELSIF _op_name = 'hive::protocol::escrow_release_operation' THEN
        r.kind := 'release';
        r.hbd_amount  := NULLIF((_body->'value'->'hbd_amount' ->>'amount'), '')::bigint;
        r.hive_amount := NULLIF((_body->'value'->'hive_amount'->>'amount'), '')::bigint;
        r.fee_amount := NULL;
        r.fee_nai    := NULL;

    ELSIF _op_name = 'hive::protocol::escrow_approved_operation' THEN
        r.kind       := 'approved';
        r.fee_amount := NULLIF((_body->'value'->'fee'->>'amount'), '')::bigint;
        r.fee_nai    := NULLIF(substring((_body->'value'->'fee'->>'nai') FROM 3), '')::smallint;
        r.hbd_amount := NULL;
        r.hive_amount:= NULL;

    ELSIF _op_name = 'hive::protocol::escrow_rejected_operation' THEN
        r.kind := 'rejected';
        r.hbd_amount := NULL; r.hive_amount := NULL; r.fee_amount := NULL; r.fee_nai := NULL;

    ELSE
        r.kind := NULL;
    END IF;

    RETURN r;
END;
$$;

RESET ROLE;
