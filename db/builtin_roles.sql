-- Role involved into data schema creation.
DO $$
BEGIN
  CREATE ROLE btracker_owner WITH LOGIN INHERIT IN ROLE hive_applications_owner_group;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

DO $$
BEGIN
  CREATE ROLE btracker_user WITH LOGIN INHERIT IN ROLE hive_applications_group;  
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

--- Allow to create schemas
GRANT btracker_owner TO haf_admin;
GRANT btracker_user TO btracker_owner;

ALTER ROLE btracker_user SET statement_timeout = '10s';
