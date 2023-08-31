DROP SCHEMA IF EXISTS btracker_endpoints CASCADE;

CREATE SCHEMA IF NOT EXISTS btracker_endpoints AUTHORIZATION btracker_owner;

SET ROLE btracker_owner;

--- Additional code here

RESET ROLE;
