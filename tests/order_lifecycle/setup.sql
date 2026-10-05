\set ON_ERROR_STOP on

-- This file runs only in run.sh's disposable HAF container. Accounts, block
-- timestamps and operation payloads are fixtures; HAF and application functions
-- are loaded from their actual installed/repository sources, without SQL mocks.
DO $$ BEGIN
  EXECUTE format('GRANT CREATE ON DATABASE %I TO hive_applications_owner_group',current_database());
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='btracker_owner') THEN
    CREATE ROLE btracker_owner IN ROLE hive_applications_owner_group;
  END IF;
END $$;
CREATE SCHEMA btracker_backend AUTHORIZATION btracker_owner;
CREATE SCHEMA btracker_endpoints AUTHORIZATION btracker_owner;
CREATE SCHEMA order59_test AUTHORIZATION btracker_owner;
SELECT hive.initialize_extension_data();
INSERT INTO hafd.operation_types (id, name, is_virtual) VALUES
(5, 'hive::protocol::limit_order_create_operation', FALSE),
(6, 'hive::protocol::limit_order_cancel_operation', FALSE),
(7, 'hive::protocol::feed_publish_operation', FALSE),
(21, 'hive::protocol::limit_order_create2_operation', FALSE),
(57, 'hive::protocol::fill_order_operation', TRUE),
(68, 'hive::protocol::hardfork_hive_operation', TRUE),
(85, 'hive::protocol::limit_order_cancelled_operation', TRUE);
BEGIN;
INSERT INTO hafd.blocks
SELECT
    n AS num,
    decode(lpad(to_hex(n), 8, '0'), 'hex') AS block_hash,
    '\x00'::BYTEA AS prev,
    TIMESTAMP '2020-01-01' + (n - 1) * INTERVAL '1 day' AS created_at,
    1 AS producer_account_id,
    '\x00'::BYTEA AS transaction_merkle_root,
    '[]'::JSONB AS extensions,
    '\x00'::BYTEA AS witness_signature,
    'STM65w' AS signing_key,
    1000 AS hbd_interest_rate,
    1000 AS total_vesting_fund_hive,
    1000000 AS total_vesting_shares,
    1000 AS total_reward_fund_hive,
    1000 AS virtual_supply,
    1000 AS current_supply,
    2000 AS current_hbd_supply,
    2000 AS dhf_interval_ledger
FROM generate_series(1, 12) AS n;
INSERT INTO hafd.accounts (id, name, block_num) VALUES (1, 'alice', 1), (2, 'bob', 1), (3, 'empty', 1);
UPDATE hafd.hive_state SET consistent_block = 12, is_dirty = FALSE, state = 'LIVE', pruning = 0;
COMMIT;

SET ROLE btracker_owner;
SET search_path TO order59_test, public;
SELECT hive.context_create('order59_test', 'order59_test');
CREATE VIEW accounts_view AS SELECT
    id,
    name
FROM hive.accounts_view;
CREATE VIEW blocks_view AS SELECT
    num,
    created_at
FROM hive.blocks_view;
CREATE TEMP TABLE _btracker_ops_batch
(id BIGINT, block_num INT, op_type_id INT, body_value JSONB);
CREATE TABLE test_checks (description TEXT NOT NULL);
CREATE SEQUENCE test_checks_count;

CREATE FUNCTION check_that(_condition BOOLEAN, _description TEXT)
RETURNS VOID LANGUAGE plpgsql SET search_path TO order59_test AS $$
BEGIN
  IF _condition IS DISTINCT FROM TRUE THEN
    RAISE EXCEPTION 'FAILED: %', _description;
  END IF;
  PERFORM nextval('test_checks_count');
  INSERT INTO test_checks VALUES (_description);
END $$;

CREATE FUNCTION asset(_amount BIGINT, _nai INT DEFAULT 21)
RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path TO order59_test AS $$
  SELECT jsonb_build_object('amount', _amount::TEXT, 'precision', 3,
    'nai', '@@' || lpad(_nai::TEXT, 9, '0'));
$$;
CREATE FUNCTION creation(
    _owner TEXT, _order BIGINT, _amount BIGINT,
    _nai INT DEFAULT 21, _fok BOOLEAN DEFAULT FALSE
)
RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path TO order59_test AS $$
  SELECT jsonb_build_object('owner', _owner, 'orderid', _order,
    'amount_to_sell', asset(_amount, _nai), 'fill_or_kill', _fok,
    'min_to_receive', asset(_amount, CASE WHEN _nai=21 THEN 13 ELSE 21 END),
    'expiration', '2020-02-01T00:00:00');
$$;
CREATE FUNCTION cancellation(_owner TEXT, _order BIGINT, _amount BIGINT, _nai INT DEFAULT 21)
RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path TO order59_test AS $$
  SELECT jsonb_build_object('seller', _owner, 'orderid', _order,
    'amount_back', asset(_amount, _nai));
$$;
CREATE FUNCTION fill(
    _open_owner TEXT, _open_order BIGINT, _open_amount BIGINT,
    _current_owner TEXT, _current_order BIGINT, _current_amount BIGINT
)
RETURNS JSONB LANGUAGE sql IMMUTABLE SET search_path TO order59_test AS $$
  SELECT jsonb_build_object('open_owner', _open_owner, 'open_orderid', _open_order,
    'open_pays', asset(_open_amount, 21), 'current_owner', _current_owner,
    'current_orderid', _current_order, 'current_pays', asset(_current_amount, 13));
$$;

RESET ROLE;
\ir ../../backend/shared/operation_types.sql
\ir ../../backend/shared/parse_amount_object.sql
\ir ../../backend/operation_parsers/market_orders.sql
\ir ../../db/order_lifecycle.sql
SET ROLE btracker_owner;
SET search_path TO order59_test, public;
SELECT install_order_lifecycle('order59_test');
SELECT check_that(
    (SELECT processed_through = 0 AND NOT backfill_required FROM order_lifecycle_status),
    'fresh installation starts ready at block zero'
);
SELECT check_that(
    (
        SELECT count(*) = 2
        FROM hafd.registered_tables
        WHERE
            origin_table_schema = 'order59_test'
            AND origin_table_name IN ('order_lifecycle', 'order_lifecycle_status')
    ),
    'actual HAF registers both lifecycle and coverage tables'
);
SELECT install_order_lifecycle('order59_test');
SELECT check_that((SELECT count(*) = 1 FROM order_lifecycle_status), 'installation is idempotent');

-- Each failure must preserve both data and coverage. This exception handler
-- tests PostgreSQL atomicity; fork.sql separately exercises HAF rewind.
CREATE FUNCTION expect_atomic_error(_sql TEXT, _message TEXT, _description TEXT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
  before_rows JSONB;
  before_status JSONB;
  failed BOOLEAN := FALSE;
BEGIN
  SELECT COALESCE(jsonb_agg(to_jsonb(l) ORDER BY create_op_id), '[]'::JSONB)
    INTO before_rows FROM order_lifecycle l;
  SELECT to_jsonb(s) INTO before_status FROM order_lifecycle_status s;
  BEGIN
    EXECUTE _sql;
  EXCEPTION WHEN OTHERS THEN
    IF position(_message IN SQLERRM) = 0 THEN
      RAISE EXCEPTION 'Unexpected failure for %: %', _description, SQLERRM;
    END IF;
    failed := TRUE;
  END;
  PERFORM check_that(failed, _description || ': rejected');
  PERFORM check_that(before_rows =
    (SELECT COALESCE(jsonb_agg(to_jsonb(l) ORDER BY create_op_id), '[]'::JSONB)
       FROM order_lifecycle l), _description || ': rows atomic');
  PERFORM check_that(before_status = (SELECT to_jsonb(s) FROM order_lifecycle_status s),
    _description || ': watermark atomic');
END $$;
