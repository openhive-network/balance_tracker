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
      type: string
      description: Asset balance for that account
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.ranked_holder CASCADE;
CREATE TYPE btracker_backend.ranked_holder AS (
    "rank" INT,
    "account" TEXT,
    "value" TEXT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.top_holders:
  type: object
  properties:
    total_accounts:
      type: integer
      description: Total number of accounts that match
    total_pages:
      type: integer
      description: Total number of pages (given requested page-size)
    holders_result:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.ranked_holder'
      description: Ranked holders for the requested page
*/
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.top_holders CASCADE;
CREATE TYPE btracker_backend.top_holders AS (
    "total_accounts" INT,
    "total_pages" INT,
    "holders_result" btracker_backend.ranked_holder[]
);
-- openapi-generated-code-end

