\set ON_ERROR_STOP on
SET search_path TO order59_test;
\ir ../../endpoints/types/coin_type.sql
\ir ../../backend/endpoint_helpers/shared_functions/exceptions.sql
\ir ../../backend/endpoint_helpers/shared_functions/validators.sql
\ir ../../backend/endpoint_helpers/shared_functions/account.sql
\ir ../../backend/endpoint_helpers/shared_functions/sync_status.sql
\ir ../../endpoints/types/order_stats.sql
\ir ../../backend/endpoint_helpers/get_order_stats/views.sql
\ir ../../backend/endpoint_helpers/get_order_stats/order_stats.sql
\ir ../../endpoints/orders/get_order_stats.sql
\ir ../../endpoints/orders/get_account_order_stats.sql
SET ROLE btracker_owner;
SET search_path TO order59_test;

DO $$
DECLARE r btracker_backend.order_stats;
BEGIN
  r := btracker_endpoints.get_order_stats();
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (16::BIGINT,5::BIGINT,9::BIGINT,2::BIGINT),
    'API aggregates actual reducer history with mutually exclusive outcomes');
  PERFORM check_that(r.created = r.filled + r.canceled + r.open AND r.fill_rate_pct = 31.250,
    'global reconciliation and three decimal fill rate');
  PERFORM check_that((r.indexed_through_block,r.indexed_at,r.coverage_from_block) =
    (4,TIMESTAMP '2020-01-04',1), 'API watermark and timestamp come from actual lifecycle views');
  PERFORM check_that(pg_typeof(r.created) = 'bigint'::REGTYPE AND
    jsonb_typeof(to_jsonb(r)->'created') = 'number', 'counts keep BIGINT and numeric JSON types');
  r := btracker_endpoints.get_account_order_stats('alice');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (10::BIGINT,2::BIGINT,7::BIGINT,1::BIGINT)
    AND r.fill_rate_pct = 20.000, 'account API counts every reused-ID incarnation');
  r := btracker_endpoints.get_order_stats(NULL,'2020-01-01');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (7::BIGINT,3::BIGINT,4::BIGINT,0::BIGINT),
    'historical creation cohort includes later fills, expiry and HF23 clearing');
  PERFORM check_that(current_setting('response.headers')::JSONB =
    '[{"Cache-Control":"public, max-age=2"}]'::JSONB, 'historical cohorts retain short cache');
  r := btracker_endpoints.get_order_stats('2020-01-02','2020-01-02');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (6::BIGINT,1::BIGINT,4::BIGINT,1::BIGINT)
    AND r.fill_rate_pct = 16.667, 'inclusive equal dates and fractional rounding');
  r := btracker_endpoints.get_account_order_stats('alice','2020-01-02','2020-01-02');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (4::BIGINT,0::BIGINT,4::BIGINT,0::BIGINT),
    'account and creation-date predicates compose');
  PERFORM check_that(current_setting('response.headers')::JSONB =
    '[{"Cache-Control":"public, max-age=2"}]'::JSONB, 'account endpoint short cache');
  r := btracker_endpoints.get_account_order_stats('empty');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (0::BIGINT,0::BIGINT,0::BIGINT,0::BIGINT)
    AND r.fill_rate_pct IS NULL, 'existing empty account returns zeros and null rate');
  r := btracker_endpoints.get_order_stats('2030-01-01',NULL);
  PERFORM check_that(r.created = 0 AND r.fill_rate_pct IS NULL, 'empty date cohort returns null rate');
END $$;

CREATE FUNCTION expect_api_error(_sql TEXT, _code TEXT, _description TEXT)
RETURNS VOID LANGUAGE plpgsql SET search_path TO order59_test AS $$
DECLARE failed BOOLEAN := FALSE;
BEGIN
  BEGIN
    EXECUTE _sql;
  EXCEPTION WHEN OTHERS THEN
    IF SQLSTATE <> _code THEN
      RAISE EXCEPTION 'Unexpected SQLSTATE % for %: %', SQLSTATE, _description, SQLERRM;
    END IF;
    failed := TRUE;
  END;
  PERFORM check_that(failed, _description);
END $$;
SELECT expect_api_error(
    $q$SELECT btracker_endpoints.get_order_stats('2020-01-03','2020-01-02')$q$,
    'PT400', 'inverted creation-date range rejected'
);
SELECT expect_api_error(
    $q$SELECT btracker_endpoints.get_account_order_stats('absent')$q$,
    'P0001', 'unknown account uses actual standard account validation'
);
SELECT expect_api_error(
    $q$SELECT btracker_endpoints.get_account_order_stats('')$q$,
    'P0001', 'empty required account rejected'
);

BEGIN;
UPDATE order_lifecycle_status SET backfill_required = TRUE;
SELECT expect_api_error('SELECT btracker_endpoints.get_order_stats()', 'PT503', 'pending backfill fails closed');
UPDATE order_lifecycle_status SET backfill_required = FALSE, processed_through = 3;
SELECT expect_api_error(
    $q$SELECT btracker_endpoints.get_account_order_stats('alice')$q$,
    'PT503', 'index behind context fails closed'
);
UPDATE order_lifecycle_status SET processed_through = 5;
SELECT expect_api_error('SELECT btracker_endpoints.get_order_stats()', 'PT503', 'index ahead of context fails closed');
DELETE FROM order_lifecycle_status;
SELECT expect_api_error('SELECT btracker_endpoints.get_order_stats()', 'PT503', 'missing coverage row fails closed');
ROLLBACK;

BEGIN;
UPDATE order_lifecycle_status SET processed_through = 0, indexed_at = NULL;
UPDATE hafd.contexts SET current_block_num = 0
WHERE name = 'order59_test';
DO $$
DECLARE r btracker_backend.order_stats := btracker_endpoints.get_order_stats();
BEGIN
  PERFORM check_that(r.created = 0 AND r.fill_rate_pct IS NULL AND r.indexed_through_block = 0
    AND r.indexed_at IS NULL AND r.coverage_from_block = 1, 'fresh block-zero metadata');
END $$;
-- The fixture change and its check are intentionally rolled back together.
ROLLBACK;

-- Physical removal of creation blocks must not change already indexed cohorts.
BEGIN;
RESET ROLE;
DELETE FROM hafd.blocks
WHERE num = 2;
SET ROLE btracker_owner;
DO $$
DECLARE r btracker_backend.order_stats;
BEGIN
  r := btracker_endpoints.get_order_stats('2020-01-02','2020-01-02');
  PERFORM check_that((r.created,r.filled,r.canceled,r.open) = (6::BIGINT,1::BIGINT,4::BIGINT,1::BIGINT),
    'persisted creation timestamps keep cohorts complete after raw source pruning');
END $$;
ROLLBACK;
