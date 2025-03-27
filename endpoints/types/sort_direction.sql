SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.sort_direction:
  type: string
  enum:
    - asc
    - desc
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.sort_direction CASCADE;
CREATE TYPE btracker_backend.sort_direction AS ENUM (
    'asc',
    'desc'
);
-- openapi-generated-code-end

RESET ROLE;
