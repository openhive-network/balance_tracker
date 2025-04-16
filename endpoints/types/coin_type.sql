SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.nai_type:
  type: string
  enum:
    - HBD
    - HIVE
    - VESTS
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.nai_type CASCADE;
CREATE TYPE btracker_backend.nai_type AS ENUM (
    'HBD',
    'HIVE',
    'VESTS'
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.balance_type:
  type: string
  enum:
    - balance
    - savings_balance
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.balance_type CASCADE;
CREATE TYPE btracker_backend.balance_type AS ENUM (
    'balance',
    'savings_balance'
);
-- openapi-generated-code-end


RESET ROLE;
