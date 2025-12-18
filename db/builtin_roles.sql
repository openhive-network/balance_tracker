/**
 * Database Roles for Balance Tracker
 * ===================================
 *
 * This file defines the two database roles required by Balance Tracker:
 *
 * btracker_owner
 * --------------
 * - Owner role with full read/write access to all Balance Tracker schemas
 * - Used by: Block processing scripts, schema migrations, admin operations
 * - Inherits from: hive_applications_owner_group (HAF role for app owners)
 * - Can create schemas, tables, functions, and process blocks
 *
 * btracker_user
 * -------------
 * - Read-only role for API access
 * - Used by: PostgREST (API server), read-only queries
 * - Inherits from: hive_applications_group (HAF role for app users)
 * - Has 10-second query timeout to prevent runaway queries
 * - Can only SELECT from tables, cannot modify data
 *
 * Role Hierarchy:
 * ---------------
 *   haf_admin
 *       └── btracker_owner (schema owner, block processor)
 *               └── btracker_user (API read-only access)
 *
 * Security Model:
 * ---------------
 * - All schema objects are owned by btracker_owner
 * - btracker_user is granted SELECT on tables after installation
 * - PostgREST connects as btracker_user for safe API access
 * - Block processing runs as btracker_owner for write access
 */

-- Create schema owner role (used for migrations and block processing)
DO $$
BEGIN
  CREATE ROLE btracker_owner WITH LOGIN INHERIT IN ROLE hive_applications_owner_group;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

-- Create API user role (read-only access via PostgREST)
DO $$
BEGIN
  CREATE ROLE btracker_user WITH LOGIN INHERIT IN ROLE hive_applications_group;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

-- Allow haf_admin to act as btracker_owner (for installation scripts)
GRANT btracker_owner TO haf_admin;

-- Allow btracker_owner to act as btracker_user (for testing)
GRANT btracker_user TO btracker_owner;

-- Limit API query execution time to prevent resource exhaustion
ALTER ROLE btracker_user SET statement_timeout = '10s';
