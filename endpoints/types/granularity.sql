SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_endpoints.granularity:
  type: string
  enum:
    - daily
    - monthly
    - yearly
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_endpoints.granularity CASCADE;
CREATE TYPE btracker_endpoints.granularity AS ENUM (
    'daily',
    'monthly',
    'yearly'
);
-- openapi-generated-code-end

RESET ROLE;
