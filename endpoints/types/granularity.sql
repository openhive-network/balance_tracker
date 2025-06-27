SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.granularity:
  type: string
  enum:
    - daily
    - monthly
    - yearly
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.granularity CASCADE;
CREATE TYPE btracker_backend.granularity AS ENUM (
    'daily',
    'monthly',
    'yearly'
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.granularity_hourly:
  type: string
  enum:
    - hourly
    - daily
    - monthly
    - yearly
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.granularity_hourly CASCADE;
CREATE TYPE btracker_backend.granularity_hourly AS ENUM (
    'hourly',
    'daily',
    'monthly',
    'yearly'
);
-- openapi-generated-code-end

RESET ROLE;
