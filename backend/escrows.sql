SET ROLE btracker_owner;

-- 1) Aggregate record type (single row per op)
DROP TYPE IF EXISTS btracker_backend.escrow_event_agg CASCADE;
CREATE TYPE btracker_backend.escrow_event_agg AS (
    from_name   text,
    to_name     text,
    agent_name  text,
    escrow_id   bigint,
    kind        text,        -- 'transfer' | 'release' | 'approved' | 'rejected'
    hbd_amount  bigint,      -- millis (transfer/release)
    hive_amount bigint,      -- millis (transfer/release)
    fee_amount  bigint,      -- millis (transfer: agent fee, approved: approval fee)
    fee_nai     smallint     -- 13 or 21 for the fee (if present; shim ignores for nai)
);

-- 2) Parser function used by the shim
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_event_agg(
    _body    jsonb,
    _op_name text
) RETURNS btracker_backend.escrow_event_agg
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    r btracker_backend.escrow_event_agg;
BEGIN
    -- common fields
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
        r.hbd_amount := NULL;
        r.hive_amount:= NULL;
        r.fee_amount := NULL;
        r.fee_nai    := NULL;

    ELSE
        -- ignored upstream (approve_intent / dispute)
        r.kind := NULL;
    END IF;

    RETURN r;
END;
$$;

-- 3) Row type expected by the processor
DROP TYPE IF EXISTS btracker_backend.escrow_event CASCADE;
CREATE TYPE btracker_backend.escrow_event AS
(
    from_name   TEXT,
    to_name     TEXT,
    agent_name  TEXT,
    escrow_id   BIGINT,
    nai         SMALLINT,   -- 13=HBD, 21=HIVE (NULL for non-amount events like 'approved')
    amount      BIGINT,     -- millis; NULL for non-amount events
    kind        TEXT,       -- 'transfer' | 'release' | 'approved' | 'rejected'
    fee         BIGINT,     -- transfer-time agent fee OR approval fee (millis)
    fee_nai     SMALLINT    -- 13 or 21 for fee (if present); informative
);

-- 4) Shim returning SETOF escrow_event (ignores nai for 'approved')
CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_events(
    _body    jsonb,
    _op_name text
) RETURNS SETOF btracker_backend.escrow_event
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  a RECORD;  -- do not depend on the agg type at compile time
  r btracker_backend.escrow_event;
BEGIN
  SELECT * INTO a
  FROM btracker_backend.get_escrow_event_agg(_body, _op_name);

  IF a.kind IS NULL THEN
    RETURN;
  END IF;

  IF a.kind IN ('transfer','release') THEN
    IF COALESCE(a.hbd_amount, 0) > 0 THEN
      r := (a.from_name, a.to_name, a.agent_name, a.escrow_id,
            13::smallint, a.hbd_amount, a.kind,
            CASE WHEN a.kind='transfer' THEN a.fee_amount ELSE NULL::bigint END,
            CASE WHEN a.kind='transfer' THEN a.fee_nai    ELSE NULL::smallint END);
      RETURN NEXT r;
    END IF;

    IF COALESCE(a.hive_amount, 0) > 0 THEN
      r := (a.from_name, a.to_name, a.agent_name, a.escrow_id,
            21::smallint, a.hive_amount, a.kind,
            CASE WHEN a.kind='transfer' THEN a.fee_amount ELSE NULL::bigint END,
            CASE WHEN a.kind='transfer' THEN a.fee_nai    ELSE NULL::smallint END);
      RETURN NEXT r;
    END IF;

  ELSIF a.kind = 'approved' THEN
    -- Emit only when a fee exists; ignore nai for 'approved'
    IF a.fee_amount IS NOT NULL THEN
      r := (a.from_name, a.to_name, a.agent_name, a.escrow_id,
            NULL::smallint,         -- ignore asset for 'approved'
            NULL::bigint,           -- no amount for 'approved'
            'approved',
            a.fee_amount,
            a.fee_nai);             -- keep fee_nai as-is (may be NULL)
      RETURN NEXT r;
    END IF;

  ELSIF a.kind = 'rejected' THEN
    r := (a.from_name, a.to_name, a.agent_name, a.escrow_id,
          NULL::smallint, NULL::bigint, 'rejected',
          NULL::bigint, NULL::smallint);
    RETURN NEXT r;
  END IF;

  RETURN;
END;
$$;

RESET ROLE;
