-- noqa: disable=AL01, AM05, LT02

SET ROLE btracker_owner;

/*
Return type for the Daily Active Users aggregation.
*/
DROP TYPE IF EXISTS btracker_backend.active_users_return CASCADE;
CREATE TYPE btracker_backend.active_users_return AS (
    bucket           TIMESTAMP,
    active_accounts  INT,
    operations       BIGINT
);

/*
Map dau_op_class API enum -> internal op_class SMALLINT.
Returns 0 for 'all', which is an internal pre-aggregated rollup.
*/
CREATE OR REPLACE FUNCTION btracker_backend.dau_op_class_id(_op_class btracker_backend.dau_op_class)
RETURNS SMALLINT
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN (
    CASE _op_class
      WHEN 'post'        THEN 1
      WHEN 'comment'     THEN 2
      WHEN 'vote'        THEN 3
      WHEN 'transfer'    THEN 4
      WHEN 'custom_json' THEN 5
      WHEN 'all'         THEN 0
      ELSE NULL
    END
  )::SMALLINT;
END
$$;

/*
Map the API granularity enum -> the date_trunc field name.
*/
CREATE OR REPLACE FUNCTION btracker_backend.dau_granularity_field(_granularity btracker_backend.granularity_dau)
RETURNS TEXT
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN (
    CASE _granularity
      WHEN 'day'   THEN 'day'
      WHEN 'week'  THEN 'week'
      WHEN 'month' THEN 'month'
    END
  );
END
$$;

/*
Parse the public comma-separated operation_types parameter into the internal
enum array. NULL/empty means all tracked DAU operation classes.
*/
CREATE OR REPLACE FUNCTION btracker_backend.parse_dau_op_classes(_operation_types TEXT)
RETURNS btracker_backend.dau_op_class[]
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
DECLARE
  __classes btracker_backend.dau_op_class[];
BEGIN
  IF _operation_types IS NULL OR btrim(_operation_types) = '' THEN
    RETURN ARRAY['all']::btracker_backend.dau_op_class[];
  END IF;

  SELECT array_agg(DISTINCT lower(btrim(value))::btracker_backend.dau_op_class)
  INTO __classes
  FROM regexp_split_to_table(_operation_types, ',') AS value
  WHERE btrim(value) <> '';

  RETURN COALESCE(__classes, ARRAY['all']::btracker_backend.dau_op_class[]);
EXCEPTION
  WHEN invalid_text_representation THEN
    RAISE EXCEPTION
      'Invalid operation_types: %. Allowed values are post, comment, vote, transfer, custom_json, all',
      _operation_types
      USING ERRCODE = '22023';
END
$$;

/*
Gap-filled DAU aggregation. Reads from the table matching the requested
granularity (active_users_by_day/_week/_month), so full-chain week/month
queries do not re-distinct years of daily rows at request time.

_op_classes: NULL, empty, or an array containing 'all' -> internal op_class=0
rollup. Any other value filters by the matching SMALLINT op_class ids.
*/
CREATE OR REPLACE FUNCTION btracker_backend.get_daily_active_users(
    _granularity btracker_backend.granularity_dau,
    _from        TIMESTAMP,
    _to          TIMESTAMP,
    _op_classes  btracker_backend.dau_op_class[]
)
RETURNS SETOF btracker_backend.active_users_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql'
STABLE
AS
$$
DECLARE
  __field          TEXT      := btracker_backend.dau_granularity_field(_granularity);
  __one_period     INTERVAL  := ('1 ' || __field)::INTERVAL;
  __from_bucket    TIMESTAMP := date_trunc(__field, _from);
  __to_bucket      TIMESTAMP := date_trunc(__field, _to);
  __source_table   TEXT;
  __filter_classes SMALLINT[];
  __single_class   SMALLINT;
BEGIN
  __source_table := (
    CASE _granularity
      WHEN 'day'   THEN 'active_users_by_day'
      WHEN 'week'  THEN 'active_users_by_week'
      WHEN 'month' THEN 'active_users_by_month'
    END
  );

  -- Normalize the op_class filter. 'all' (or NULL / empty array) uses the
  -- pre-aggregated op_class=0 rollup.
  IF _op_classes IS NULL
     OR cardinality(_op_classes) = 0
     OR 'all' = ANY(_op_classes) THEN
    __filter_classes := ARRAY[0]::SMALLINT[];
  ELSE
    SELECT array_agg(DISTINCT btracker_backend.dau_op_class_id(c) ORDER BY btracker_backend.dau_op_class_id(c))
    INTO __filter_classes
    FROM unnest(_op_classes) AS c
    WHERE btracker_backend.dau_op_class_id(c) IS NOT NULL;
  END IF;

  IF cardinality(__filter_classes) = 1 THEN
    __single_class := __filter_classes[1];
  END IF;

  RETURN QUERY EXECUTE format(
  $SQL$
    WITH date_series AS (
      SELECT generate_series(
          $1::TIMESTAMP,
          $2::TIMESTAMP,
          $4::INTERVAL
      ) AS bucket
    ),
    rolled AS MATERIALIZED (
      SELECT
        au.bucket,
        (CASE
          WHEN $5::SMALLINT IS NOT NULL THEN COUNT(*)
          ELSE COUNT(DISTINCT au.account)
        END)::INT AS active_accounts,
        SUM(au.operations_count)::BIGINT AS operations
      FROM %I au
      WHERE au.bucket BETWEEN $1::TIMESTAMP AND $2::TIMESTAMP
        AND ($3::SMALLINT[] IS NULL OR au.op_class = ANY($3::SMALLINT[]))
      GROUP BY au.bucket
    )
    SELECT
      ds.bucket,
      COALESCE(r.active_accounts, 0)::INT  AS active_accounts,
      COALESCE(r.operations,      0)::BIGINT AS operations
    FROM date_series ds
    LEFT JOIN rolled r ON r.bucket = ds.bucket
    ORDER BY ds.bucket ASC;
  $SQL$,
  __source_table
  )
  USING __from_bucket, __to_bucket, __filter_classes, __one_period, __single_class;
END
$$;

RESET ROLE;
