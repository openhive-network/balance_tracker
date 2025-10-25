SET ROLE btracker_owner;

DROP TYPE IF EXISTS btracker_backend.escrow_event CASCADE;
CREATE TYPE btracker_backend.escrow_event AS
(
    from_name   TEXT,
    to_name     TEXT,
    agent_name  TEXT,
    escrow_id   BIGINT,
    nai         SMALLINT,   -- 13=HBD, 21=HIVE (NULL for non-amount events like 'approved'/'rejected')
    amount      BIGINT,     -- millis; NULL for non-amount events
    kind        TEXT,       -- 'transfer' | 'release' | 'approved' | 'rejected'
    fee         BIGINT,     -- transfer-time agent fee OR approval fee (millis)
    fee_nai     SMALLINT    -- 13 or 21 for fee (if present); informative
); 

CREATE OR REPLACE FUNCTION btracker_backend.get_escrow_events(
    _body    jsonb,
    _op_name text
) RETURNS btracker_backend.escrow_event
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    -- Common fields
    _from   text    := (_body->'value'->>'from');
    _to     text    := (_body->'value'->>'to');
    _agent  text    := (_body->'value'->>'agent');
    _id     bigint  := NULLIF((_body->'value'->>'escrow_id'), '')::bigint;

    -- Amounts / fee
    _hbd_amt   bigint    := NULLIF((_body->'value'->'hbd_amount' ->>'amount'), '')::bigint;
    _hive_amt  bigint    := NULLIF((_body->'value'->'hive_amount'->>'amount'), '')::bigint;
    _fee_amt   bigint    := NULLIF((_body->'value'->'fee'->>'amount'), '')::bigint;
    _fee_nai   smallint  := NULLIF(substring((_body->'value'->'fee'->>'nai') FROM 3), '')::smallint;

    _kind   text;
    _nai    smallint;
    _amt    bigint;

    r       btracker_backend.escrow_event;
BEGIN
    IF _op_name = 'hive::protocol::escrow_transfer_operation' THEN
        _kind := 'transfer';
        IF COALESCE(_hbd_amt,0) > 0 THEN
            _nai := 13; _amt := _hbd_amt;
        ELSIF COALESCE(_hive_amt,0) > 0 THEN
            _nai := 21; _amt := _hive_amt;
        ELSE
            _nai := NULL; _amt := NULL;
        END IF;

    ELSIF _op_name = 'hive::protocol::escrow_release_operation' THEN
        _kind := 'release';
        _fee_amt := NULL; _fee_nai := NULL;  -- no fee on release
        IF COALESCE(_hbd_amt,0) > 0 THEN
            _nai := 13; _amt := _hbd_amt;
        ELSIF COALESCE(_hive_amt,0) > 0 THEN
            _nai := 21; _amt := _hive_amt;
        ELSE
            _nai := NULL; _amt := NULL;
        END IF;

    ELSIF _op_name = 'hive::protocol::escrow_approved_operation' THEN
        _kind := 'approved';
        -- only fee applies on approved
        _nai := NULL; _amt := NULL;

    ELSIF _op_name = 'hive::protocol::escrow_rejected_operation' THEN
        _kind := 'rejected';
        _nai := NULL; _amt := NULL;
        _fee_amt := NULL; _fee_nai := NULL;

    ELSE
        -- ignored kinds (approve_intent / dispute / etc.)
        RETURN NULL;
    END IF;

   
    r := (_from, _to, _agent, _id, _nai, _amt, _kind, _fee_amt, _fee_nai);
    RETURN r;
END;
$$;

RESET ROLE;
