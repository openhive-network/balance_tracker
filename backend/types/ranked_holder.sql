SET ROLE btracker_owner;

-- Type definitions for top holders endpoint.
-- These types live in backend/ because backend/endpoint_helpers/top_holders.sql
-- depends on them at function creation time. The endpoints/types/ranked_holder.sql
-- file contains the corresponding OpenAPI annotations and will recreate these
-- types (DROP CASCADE + CREATE) when it runs later in the install sequence.

DROP TYPE IF EXISTS btracker_backend.top_holders CASCADE;
DROP TYPE IF EXISTS btracker_backend.ranked_holder CASCADE;

CREATE TYPE btracker_backend.ranked_holder AS (
    "rank" INT,
    "account" TEXT,
    "value" TEXT
);

CREATE TYPE btracker_backend.top_holders AS (
    "total_accounts" INT,
    "total_pages" INT,
    "holders_result" btracker_backend.ranked_holder[]
);

RESET ROLE;
