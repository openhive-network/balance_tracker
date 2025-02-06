docker exec -i haf-instance psql -U haf_admin -d haf_block_log < test_balances.sql


docker exec -i haf-instance psql -U haf_admin -d haf_block_log << EOF
SET search_path TO btracker_app, public;

-- First check if we have any data in our special balance tables
SELECT * FROM btracker_app.check_special_balances_data();

-- Find accounts with special balances
SELECT * FROM btracker_app.find_accounts_with_balances();

-- Try getting balances for a specific account
SELECT * FROM btracker_app.get_special_balances('hiveio');
EOF