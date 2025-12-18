SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.total_value_locked:
  type: object
  properties:
    block_num:
      type: integer
      description: Head block number at which the snapshot was computed
    total_vests:
      type: string
      description: Global sum of VESTS 
    savings_hive:
      type: string
      description: Total number of HIVE in savings 
    savings_hbd:
      type: string
      description: Total number of HIVE backed dollars the chain has in savings
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.total_value_locked CASCADE;
CREATE TYPE btracker_backend.total_value_locked AS (
    "block_num" INT,
    "total_vests" TEXT,
    "savings_hive" TEXT,
    "savings_hbd" TEXT
);
-- openapi-generated-code-end

RESET ROLE;
