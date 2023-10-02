-- Role involved into data schema creation.
DO $$
BEGIN
  CREATE ROLE btracker_owner WITH LOGIN INHERIT IN ROLE hive_applications_group;

  CREATE ROLE btracker_user WITH LOGIN INHERIT IN ROLE hive_applications_group;
  
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

--- Allow to create schemas
GRANT CREATE ON DATABASE haf_block_log TO btracker_owner;
GRANT btracker_owner TO haf_admin;
GRANT btracker_owner TO haf_app_admin;
GRANT btracker_user TO btracker_owner;
