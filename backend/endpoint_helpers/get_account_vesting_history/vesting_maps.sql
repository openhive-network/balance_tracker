SET ROLE btracker_owner;

/*
 * Vesting read-side mappers + event projection.
 * --------------------------------------------------------------------------
 * Keeps the kind<->op_type / kind<->filter mappings and the vesting_history_event
 * construction in ONE place, so get_account_vesting_history (and any future vesting
 * reader) shows only its essential logic instead of inlining the same CASE blocks.
 * The event "kinds" themselves are the btracker_backend.vesting_kind_*() constants
 * (backend/shared/operation_types.sql).
 */

-- vesting_filter (API enum) -> internal kind. 'all' maps to NULL (no kind filter).
CREATE OR REPLACE FUNCTION btracker_backend.vesting_filter_to_kind(_filter btracker_backend.vesting_filter)
RETURNS SMALLINT
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN CASE _filter
    WHEN 'all'                       THEN NULL
    WHEN 'power_up'                  THEN btracker_backend.vesting_kind_power_up()
    WHEN 'power_down_init'           THEN btracker_backend.vesting_kind_power_down_init()
    WHEN 'power_down_fill'           THEN btracker_backend.vesting_kind_power_down_fill()
    WHEN 'power_down_route_received' THEN btracker_backend.vesting_kind_power_down_route_received()
  END;
END
$$;

-- internal kind -> on-chain op_type_id surfaced in a history row.
-- (kinds power_down_fill and power_down_route_received both originate from op 56.)
CREATE OR REPLACE FUNCTION btracker_backend.vesting_kind_to_op_type(_kind SMALLINT)
RETURNS SMALLINT
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN CASE _kind
    WHEN btracker_backend.vesting_kind_power_up()                  THEN btracker_backend.op_transfer_to_vesting()
    WHEN btracker_backend.vesting_kind_power_down_init()           THEN btracker_backend.op_withdraw_vesting()
    WHEN btracker_backend.vesting_kind_power_down_fill()           THEN btracker_backend.op_fill_vesting_withdraw()
    WHEN btracker_backend.vesting_kind_power_down_route_received() THEN btracker_backend.op_fill_vesting_withdraw()
  END;
END
$$;

-- internal kind -> direction label (vesting_filter) carried on a history row.
CREATE OR REPLACE FUNCTION btracker_backend.vesting_kind_to_filter(_kind SMALLINT)
RETURNS btracker_backend.vesting_filter
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN (CASE _kind
    WHEN btracker_backend.vesting_kind_power_up()                  THEN 'power_up'
    WHEN btracker_backend.vesting_kind_power_down_init()           THEN 'power_down_init'
    WHEN btracker_backend.vesting_kind_power_down_fill()           THEN 'power_down_fill'
    WHEN btracker_backend.vesting_kind_power_down_route_received() THEN 'power_down_route_received'
  END)::btracker_backend.vesting_filter;
END
$$;

-- Build one vesting_history_event from a raw account_vesting_history row.
CREATE OR REPLACE FUNCTION btracker_backend.project_vesting_event(
    _block_num    INT,
    _source_op    BIGINT,
    _kind         SMALLINT,
    _hive_amount  BIGINT,
    _vests_amount NUMERIC,
    _created_at   TIMESTAMP
)
RETURNS btracker_backend.vesting_history_event
LANGUAGE 'plpgsql' STABLE
AS
$$
BEGIN
  RETURN (
    _block_num,
    _source_op::TEXT,
    btracker_backend.vesting_kind_to_op_type(_kind),
    btracker_backend.vesting_kind_to_filter(_kind),
    btracker_backend.create_amount_object(btracker_backend.nai_hive(),  _hive_amount),
    btracker_backend.create_amount_object(btracker_backend.nai_vests(), _vests_amount),
    _created_at
  )::btracker_backend.vesting_history_event;
END
$$;

RESET ROLE;
