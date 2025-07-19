/** openapi:components:schemas
btracker_backend.ranked_holder:
  type: object
  properties:
    rank:
      type: integer
      description: Position in the ranking
    account:
      type: string
      description: Account name
    value:
      type: number
      x-sql-datatype: NUMERIC
      description: Asset balance for that account
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.ranked_holder CASCADE;
CREATE TYPE btracker_backend.ranked_holder AS (
    "rank" INT,
    "account" TEXT,
    "value" NUMERIC
);
-- openapi-generated-code-end
