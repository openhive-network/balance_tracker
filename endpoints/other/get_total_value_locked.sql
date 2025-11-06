SET ROLE btracker_owner;

/** openapi:paths
/total-value-locked:
  get:
    tags:
      - Other
    summary: Returns Total Value Locked (VESTS + HIVE savings + HBD savings)
    description: |
      Computes the current chain totals of VESTS, HIVE savings, and HBD savings at the head block.
    operationId: hafbe_endpoints.get_total_value_locked
    responses:
      '200':
        description: TVL snapshot
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/hafbe_types.total_value_locked'
            example:
              {
                "block_num": 5000000,
                "total_vests": "448144916705468383",
                "savings_hive": "0",
                "savings_hbd": "0"
              }
      '404':
        description: No blocks in the database
*/
-- openapi-generated-code-begin
DROP FUNCTION IF EXISTS btracker_endpoints.get_total_value_locked;
CREATE OR REPLACE FUNCTION btracker_endpoints.get_total_value_locked()
RETURNS hafbe_types.total_value_locked
-- openapi-generated-code-end
LANGUAGE plpgsql STABLE
SET JIT = OFF
SET join_collapse_limit = 16
SET from_collapse_limit = 16
AS
$$
DECLARE
  _row btracker_backend.total_value_locked;
BEGIN
  PERFORM set_config('response.headers', '[{"Cache-Control": "public, max-age=2"}]', true);

  _row := btracker_backend.get_total_value_locked();

  RETURN _row;
END
$$;

RESET ROLE;
