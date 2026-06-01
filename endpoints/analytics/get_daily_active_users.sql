SET ROLE btracker_owner;

/** openapi:paths
/daily-active-users:
  get:
    tags:
      - Analytics
    summary: Daily Active Users (DAU)
    description: |
      Unique accounts that submitted at least one non-trivial operation in
      each period, plus the total operation count for the period.

      SQL example
      * `SELECT * FROM btracker_endpoints.get_daily_active_users();`

      REST call example
      * `GET ''https://%1$s/balance-api/daily-active-users?granularity=day&operation_types=vote,transfer''`
    operationId: btracker_endpoints.get_daily_active_users
    parameters:
      - in: query
        name: from-date
        required: false
        schema:
          type: string
          x-sql-datatype: DATE
          format: date
          default: NULL
        description: |
          Inclusive start date `YYYY-MM-DD`. Week/month granularities return
          the containing whole week/month bucket.
      - in: query
        name: to-date
        required: false
        schema:
          type: string
          x-sql-datatype: DATE
          format: date
          default: NULL
        description: |
          Inclusive end date `YYYY-MM-DD`. Defaults to today (UTC).
      - in: query
        name: granularity
        required: false
        schema:
          $ref: '#/components/schemas/btracker_backend.granularity_dau'
          default: day
        description: |
          Bucket size:

          * `day`   — daily buckets
          * `week`  — ISO-week buckets (Monday-start)
          * `month` — calendar-month buckets
      - in: query
        name: operation_types
        required: false
        schema:
          type: string
          default: all
        description: |
          Comma-separated list of operation classes to count.
          Allowed values: `post`, `comment`, `vote`, `transfer`, `custom_json`, `all`.

          `all` (the default) is equivalent to all five tracked classes — not
          every Hive op. Unknown values raise an error.

          Example: `operation_types=vote,transfer`.
    responses:
      '200':
        description: |
          Active user counts and operation counts per bucket.
        content:
          application/json:
            schema:
              type: object
              x-sql-datatype: JSONB
              properties:
                granularity:
                  $ref: '#/components/schemas/btracker_backend.granularity_dau'
                data:
                  type: array
                  items:
                    type: object
                    properties:
                      date:
                        type: string
                        format: date-time
                      active_accounts:
                        type: integer
                      operations:
                        type: integer
            example: {
              "granularity": "day",
              "data": [
                { "date": "2026-04-01T00:00:00", "active_accounts": 12450, "operations": 387200 }
              ]
            }
 */
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_daily_active_users;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_daily_active_users(
    "from-date"       DATE = NULL,
    "to-date"         DATE = NULL,
    "granularity"     btracker_backend.granularity_dau = 'day',
    "operation_types" TEXT = 'all'
)
RETURNS JSONB
-- openapi-generated-code-end
LANGUAGE 'plpgsql'
STABLE
SET jit = OFF
AS
$$
/*
================================================================================
ENDPOINT: get_daily_active_users (issue #48)
================================================================================
PURPOSE:
  Returns unique-account counts plus total op counts per bucket for the
  configured operation classes. Backs network-activity dashboards.

PARAMETERS:
  - from-date / to-date: inclusive date range. Default to 30 days ago / today.
  - granularity: 'day' | 'week' | 'month'.
  - operation_types: comma-separated dau_op_class values (or 'all').

DATA SOURCE:
  active_users_by_day/_week/_month (populated by db/process_active_users.sql).
  Reads the table matching the requested granularity so full-chain range
  requests stay bounded to the requested bucket size.

RESPONSE SHAPE (per issue #48):
  { "granularity": "<granularity>", "data": [ {date, active_accounts, operations}, ... ] }

CACHING:
  - Fully historical range (to <= last full day): 1 year cache
  - Otherwise: 2 second cache (today's bucket can still grow)
================================================================================
*/
DECLARE
  _to_date   DATE := COALESCE("to-date",   (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::DATE);
  _from_date DATE := COALESCE("from-date", _to_date - 30);
  _today     DATE := (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::DATE;
  _op_classes btracker_backend.dau_op_class[];
BEGIN
  IF _from_date > _to_date THEN
    RAISE EXCEPTION 'from (%) must not be after to (%)', _from_date, _to_date;
  END IF;

  _op_classes := btracker_backend.parse_dau_op_classes("operation_types");

  IF _to_date < _today THEN
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=31536000"}]', true);
  ELSE
    PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);
  END IF;

  RETURN (
    SELECT jsonb_build_object(
      'granularity', "granularity",
      'data', COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'date',            row.bucket,
            'active_accounts', row.active_accounts,
            'operations',      row.operations
          )
          ORDER BY row.bucket
        ),
        '[]'::jsonb
      )
    )
    FROM btracker_backend.get_daily_active_users(
      "granularity",
      _from_date::TIMESTAMP,
      (_to_date + INTERVAL '1 day' - INTERVAL '1 microsecond')::TIMESTAMP,
      _op_classes
    ) row
  );
END
$$;

RESET ROLE;
