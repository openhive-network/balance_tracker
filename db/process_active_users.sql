SET ROLE btracker_owner;

/**
 * Aggregates non-trivial submitter activity into per-(bucket, op_class, account)
 * active_users_by_day/_week/_month tables. Backs the Daily Active Users
 * endpoint (issue #48).
 *
 * Tracked op classes:
 *   0 = all          -> internal rollup across all tracked classes
 *   1 = post         -> comment_operation where parent_author = ''
 *   2 = comment      -> comment_operation where parent_author <> ''
 *   3 = vote         -> vote_operation
 *   4 = transfer     -> transfer_operation
 *   5 = custom_json  -> custom_json_operation
 *
 * Actor extraction per op-type (from body_value JSONB):
 *   - vote:        body->>'voter'
 *   - comment:     body->>'author'
 *   - transfer:    body->>'from'
 *   - custom_json: every name in required_posting_auths + required_auths
 *
 * Operations-count semantics (issue #48 "operations" field):
 *   Every chain operation contributes exactly 1 to SUM(operations_count).
 *   For single-actor ops (vote/comment/transfer) the actor row carries
 *   op_weight = 1. For multi-sig custom_json, only ONE fan-out row — the
 *   primary signer — carries op_weight = 1; co-signers carry 0. They are
 *   still stored so they count toward `active_accounts` (COUNT DISTINCT),
 *   but not toward `operations`. Primary = first element of
 *   required_posting_auths, falling back to first of required_auths.
 *
 * Virtual ops are excluded by construction (none of the tracked op types
 * are virtual).
 *
 * Plan-cache strategy:
 *   _btracker_ops_batch is dropped/recreated by btracker_prefetch_operations
 *   each batch, so any statement reading it has its cached plan invalidated
 *   per batch. We isolate that cost to a SINGLE simple filter+insert
 *   (_dau_ops) so the planner only re-plans a trivial statement; every
 *   downstream stage reads stable-OID temp tables and gets its plan cached
 *   after the first call. The expensive bits — CASE+LATERAL actor extraction,
 *   blocks_view date prefetch, accounts_view scalar subquery, GROUPING SETS
 *   aggregation, 3-way ON CONFLICT upserts — all keep cached plans across
 *   batches.
 *
 * Account-id resolution: inline correlated scalar subquery against
 * hive.accounts_view (matches process_balances:ops_in_range — per-row
 * Index Scan on the accounts name index, avoiding the !370 seq-scan trap).
 */
CREATE OR REPLACE FUNCTION process_active_users(_from INT, _to INT)
RETURNS VOID
LANGUAGE 'plpgsql' VOLATILE
SET jit = OFF
AS
$$
DECLARE
  _op_vote        INT := btracker_backend.op_vote();
  _op_comment     INT := btracker_backend.op_comment();
  _op_transfer    INT := btracker_backend.op_transfer();
  _op_custom_json INT := btracker_backend.op_custom_json();
  __by_day_count    BIGINT;
  __by_week_count   BIGINT;
  __by_month_count  BIGINT;
  __ops_rows        BIGINT;
  __actors_rows     BIGINT;
  __resolved_rows   BIGINT;
  __aggregated_rows BIGINT;
  __t0 timestamptz;
  __t1 timestamptz;
  __t2 timestamptz;
  __t3 timestamptz;
  __t4 timestamptz;
  __t5 timestamptz;
  __t6 timestamptz;
BEGIN
  __t0 := clock_timestamp();

  -- Fast path: if the batch has no DAU-relevant ops at all, skip entirely.
  IF NOT EXISTS (
    SELECT 1 FROM _btracker_ops_batch ov
    WHERE ov.op_type_id IN (_op_vote, _op_comment, _op_transfer, _op_custom_json)
      AND ov.block_num BETWEEN _from AND _to
  ) THEN
    RAISE NOTICE 'process_active_users [%, %]: no relevant ops, skipped in % ms',
      _from, _to, (extract(epoch FROM clock_timestamp() - __t0) * 1000)::INT;
    RETURN;
  END IF;

  -- =========================================================================
  -- Persistent working tables (created once per session, TRUNCATEd per batch
  -- so plans referencing them stay cached). Order matters: list every table
  -- read by a plan-cached statement.
  -- =========================================================================
  CREATE TEMP TABLE IF NOT EXISTS _dau_block_dates (
    block_num INT       PRIMARY KEY,
    by_day    TIMESTAMP NOT NULL,
    by_week   TIMESTAMP NOT NULL,
    by_month  TIMESTAMP NOT NULL
  );
  CREATE TEMP TABLE IF NOT EXISTS _dau_ops (
    id          BIGINT NOT NULL,
    body        JSONB,
    op_type_id  INT    NOT NULL,
    block_num   INT    NOT NULL
  );
  CREATE TEMP TABLE IF NOT EXISTS _dau_actors (
    block_num  INT      NOT NULL,
    op_class   SMALLINT NOT NULL,
    actor_name TEXT     NOT NULL,
    op_weight  INT      NOT NULL
  );
  CREATE TEMP TABLE IF NOT EXISTS _dau_resolved (
    by_day    TIMESTAMP NOT NULL,
    by_week   TIMESTAMP NOT NULL,
    by_month  TIMESTAMP NOT NULL,
    op_class  SMALLINT  NOT NULL,
    account   INT       NOT NULL,
    op_weight INT       NOT NULL,
    block_num INT       NOT NULL
  );
  CREATE TEMP TABLE IF NOT EXISTS _dau_aggregated (
    by_day           TIMESTAMP,
    by_week          TIMESTAMP,
    by_month         TIMESTAMP,
    op_class         SMALLINT NOT NULL,
    account          INT      NOT NULL,
    operations_count INT      NOT NULL,
    last_block_num   INT      NOT NULL,
    grp_level        INT      NOT NULL
  );
  TRUNCATE _dau_block_dates, _dau_ops, _dau_actors, _dau_resolved, _dau_aggregated;

  -- =========================================================================
  -- Stage A: prefetch block dates ONCE for the whole batch range.
  -- =========================================================================
  INSERT INTO _dau_block_dates (block_num, by_day, by_week, by_month)
  SELECT
    bv.num,
    date_trunc('day',   bv.created_at),
    date_trunc('week',  bv.created_at),
    date_trunc('month', bv.created_at)
  FROM hive.blocks_view bv
  WHERE bv.num BETWEEN _from AND _to;
  __t1 := clock_timestamp();

  -- =========================================================================
  -- Stage B: copy DAU-relevant ops out of _btracker_ops_batch into a
  -- stable-OID temp table. The ONLY statement whose plan invalidates per
  -- batch (because _btracker_ops_batch is dropped/recreated by prefetch).
  -- Deliberately simple so re-planning costs are negligible.
  -- =========================================================================
  INSERT INTO _dau_ops (id, body, op_type_id, block_num)
  SELECT ov.id, ov.body_value, ov.op_type_id, ov.block_num
  FROM _btracker_ops_batch ov
  WHERE ov.op_type_id IN (_op_vote, _op_comment, _op_transfer, _op_custom_json)
    AND ov.block_num BETWEEN _from AND _to;
  GET DIAGNOSTICS __ops_rows = ROW_COUNT;
  __t2 := clock_timestamp();

  -- =========================================================================
  -- Stage C: extract actors (CASE for vote/comment/transfer single-pass +
  -- LATERAL for custom_json auth fan-out). Reads _dau_ops (stable OID) so
  -- the planner caches this plan after the first call.
  -- =========================================================================
  INSERT INTO _dau_actors (block_num, op_class, actor_name, op_weight)
  SELECT block_num, op_class, actor_name, op_weight
  FROM (
    SELECT
      o.block_num,
      (CASE o.op_type_id
         WHEN _op_vote     THEN 3::SMALLINT
         WHEN _op_transfer THEN 4::SMALLINT
         WHEN _op_comment  THEN
           (CASE WHEN COALESCE(o.body->>'parent_author', '') = '' THEN 1 ELSE 2 END)::SMALLINT
       END) AS op_class,
      (CASE o.op_type_id
         WHEN _op_vote     THEN o.body->>'voter'
         WHEN _op_comment  THEN o.body->>'author'
         WHEN _op_transfer THEN o.body->>'from'
       END) AS actor_name,
      1 AS op_weight
    FROM _dau_ops o
    WHERE o.op_type_id IN (_op_vote, _op_comment, _op_transfer)

    UNION ALL

    SELECT
      o.block_num,
      5::SMALLINT AS op_class,
      auth.name AS actor_name,
      (CASE WHEN row_number() OVER (PARTITION BY o.id ORDER BY auth.priority, auth.idx) = 1
            THEN 1 ELSE 0 END) AS op_weight
    FROM _dau_ops o
    CROSS JOIN LATERAL (
      SELECT t.name, 1 AS priority, t.ord AS idx
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(o.body->'required_posting_auths') = 'array'
             THEN o.body->'required_posting_auths' ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS t(name, ord)
      UNION ALL
      SELECT t.name, 2 AS priority, t.ord
      FROM jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(o.body->'required_auths') = 'array'
             THEN o.body->'required_auths' ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS t(name, ord)
    ) AS auth
    WHERE o.op_type_id = _op_custom_json AND auth.name IS NOT NULL
  ) x
  WHERE x.actor_name IS NOT NULL;
  GET DIAGNOSTICS __actors_rows = ROW_COUNT;
  __t3 := clock_timestamp();

  -- =========================================================================
  -- Stage D: join actors with block dates and resolve account IDs via inline
  -- scalar subquery (matches process_balances:ops_in_range pattern).
  -- Reads only stable-OID temp tables + hive.accounts_view -> plan caches.
  -- =========================================================================
  INSERT INTO _dau_resolved (by_day, by_week, by_month, op_class, account, op_weight, block_num)
  SELECT by_day, by_week, by_month, op_class, account, op_weight, block_num
  FROM (
    SELECT
      bd.by_day, bd.by_week, bd.by_month,
      a.op_class,
      (SELECT av.id FROM hive.accounts_view av WHERE av.name = a.actor_name) AS account,
      a.op_weight,
      a.block_num
    FROM _dau_actors a
    JOIN _dau_block_dates bd ON bd.block_num = a.block_num
  ) resolved
  WHERE account IS NOT NULL;
  GET DIAGNOSTICS __resolved_rows = ROW_COUNT;
  __t4 := clock_timestamp();

  -- =========================================================================
  -- Stage E: GROUPING SETS produces per-(bucket, op_class, account) rows AND
  -- the op_class=0 rollup (NULL op_class -> coalesced to 0). Plan cached.
  -- =========================================================================
  INSERT INTO _dau_aggregated (by_day, by_week, by_month, op_class, account, operations_count, last_block_num, grp_level)
  SELECT
    by_day,
    by_week,
    by_month,
    COALESCE(op_class, 0)::SMALLINT AS op_class,
    account,
    SUM(op_weight)::INT AS operations_count,
    MAX(block_num)::INT AS last_block_num,
    GROUPING(by_day, by_week, by_month) AS grp_level
  FROM _dau_resolved
  GROUP BY GROUPING SETS (
    (by_day,   op_class, account),
    (by_day,             account),
    (by_week,  op_class, account),
    (by_week,            account),
    (by_month, op_class, account),
    (by_month,           account)
  );
  GET DIAGNOSTICS __aggregated_rows = ROW_COUNT;
  __t5 := clock_timestamp();

  -- =========================================================================
  -- Stages F/G/H: upsert each grain. Plans cached. grp_level bitmask:
  --   3 = day, 5 = week, 6 = month
  -- =========================================================================
  INSERT INTO active_users_by_day AS au
    (bucket, op_class, account, operations_count, last_block_num)
  SELECT by_day, op_class, account, operations_count, last_block_num
  FROM _dau_aggregated
  WHERE grp_level = 3
  ON CONFLICT ON CONSTRAINT pk_active_users_by_day DO UPDATE SET
    operations_count = au.operations_count + EXCLUDED.operations_count,
    last_block_num   = GREATEST(au.last_block_num, EXCLUDED.last_block_num);
  GET DIAGNOSTICS __by_day_count = ROW_COUNT;

  INSERT INTO active_users_by_week AS au
    (bucket, op_class, account, operations_count, last_block_num)
  SELECT by_week, op_class, account, operations_count, last_block_num
  FROM _dau_aggregated
  WHERE grp_level = 5
  ON CONFLICT ON CONSTRAINT pk_active_users_by_week DO UPDATE SET
    operations_count = au.operations_count + EXCLUDED.operations_count,
    last_block_num   = GREATEST(au.last_block_num, EXCLUDED.last_block_num);
  GET DIAGNOSTICS __by_week_count = ROW_COUNT;

  INSERT INTO active_users_by_month AS au
    (bucket, op_class, account, operations_count, last_block_num)
  SELECT by_month, op_class, account, operations_count, last_block_num
  FROM _dau_aggregated
  WHERE grp_level = 6
  ON CONFLICT ON CONSTRAINT pk_active_users_by_month DO UPDATE SET
    operations_count = au.operations_count + EXCLUDED.operations_count,
    last_block_num   = GREATEST(au.last_block_num, EXCLUDED.last_block_num);
  GET DIAGNOSTICS __by_month_count = ROW_COUNT;
  __t6 := clock_timestamp();

  RAISE NOTICE 'process_active_users [%, %]: blocks=% ops=%/% actors=%/% resolved=%/% agg=%/% upsert=%/%/%/% total=% ms',
    _from, _to,
    (extract(epoch FROM __t1 - __t0) * 1000)::INT,
    __ops_rows,        (extract(epoch FROM __t2 - __t1) * 1000)::INT,
    __actors_rows,     (extract(epoch FROM __t3 - __t2) * 1000)::INT,
    __resolved_rows,   (extract(epoch FROM __t4 - __t3) * 1000)::INT,
    __aggregated_rows, (extract(epoch FROM __t5 - __t4) * 1000)::INT,
    __by_day_count, __by_week_count, __by_month_count,
    (extract(epoch FROM __t6 - __t5) * 1000)::INT,
    (extract(epoch FROM __t6 - __t0) * 1000)::INT;
END
$$;

RESET ROLE;
