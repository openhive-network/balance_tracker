# Pipeline Job Failure Analysis

**Date:** 2025-12-08
**Analyzed Pipelines:**
- HAF 141702 (commit 1892ff8a)
- balance_tracker 141713 (commit b57669df)
- reputation_tracker 141690 (commit 45bbe24)
- HAfAH 141726 (commit 5dd431a)

---

## HAF Pipeline 141702 Failed Jobs

### 1. prepare_haf_data (Job ID: 2368173)

**Status:** TIMEOUT (1 hour limit exceeded)

**Root Cause:** **Local disk copy before NFS push consumed remaining time.** The replay completed in ~56 minutes (3340 seconds), leaving only ~4 minutes for cache operations. The cache-manager.sh `cmd_put` was doing:
1. `cp -a` from `/cache/replay_data_haf_X` to `/cache/haf_X` (local cache copy)
2. Then `rsync` to `/nfs/ci-cache/haf/X` (NFS push - never reached)

The job timed out during the local copy (step 1), before even attempting NFS.

**Evidence:**
```
[cache-manager] Relaxing pgdata permissions for caching
[cache-manager] Caching locally: /cache/haf_1892ff8a074a9438331a208934b006db1601fc82
Warning: Failed to push to NFS cache
section_end:...:step_script
WARNING: step_script could not run to completion because the timeout was exceeded
ERROR: Job failed: execution took longer than 1h0m0s seconds
```

Key observations:
- "Caching locally" message appears (local copy started)
- No "Copying to NFS" message (NFS push never started)
- Timeout hit during `step_script` section, not `after_script`

**Why is the copy slow inside Docker vs on host?**

Tested on hive-builder-9:
- **Host direct**: `cp -a` of 9.9GB (1376 files) takes **3.6 seconds**
- **Host to NFS**: `cp -a` of 9.9GB to NFS takes **40 seconds**
- **Total from host**: ~44 seconds

But the job runs `cache-manager.sh` inside the Docker executor container. Even though `/cache` is a bind mount from the host, operations inside the container have overhead from:
1. Container filesystem layer traversal
2. Docker storage driver (overlay2) for metadata operations
3. The `chmod -R a+rX` on pgdata (many inodes to update)

The job timeout hit while cache-manager was running, killing both the `chmod -R` and `cp -a` processes.

**The current fix eliminates the redundant local copy by:**
- Skipping local cache copy when NFS is available
- Going directly to NFS push (the shared cache that matters)
- Using symlink (instant) for local cache reference after NFS push succeeds

**Relation to CI Changes:** The cache-manager.sh design was flawed - it copied to local cache BEFORE checking/using NFS, wasting the limited remaining time.

**Fix Applied:** Modified cache-manager.sh to:
1. Skip local cache copy when NFS is available
2. Only use local cache as fallback when NFS is unavailable
3. Create symlink (instant) instead of copy for local cache reference after NFS push succeeds

**Additional Recommendation - Run Copy Outside Docker:**

The copy operations should ideally run directly on the host, not inside the Docker container. On the host:
- Local copy: 3.6 seconds
- NFS copy: 40 seconds
- **Total: ~44 seconds**

Options to achieve this:
1. **Increase job timeout** to 70 minutes (immediate fix, +10 minutes buffer)
2. **Run cache-manager via docker exec on host** - Use dind to execute on the host filesystem directly
3. **Use a separate shell executor job** - Trigger a follow-up job that uses shell executor instead of Docker
4. **Move cache push to GitLab CI cache mechanism** - Use GitLab's built-in cache with custom paths

---

### 2. replay_filtered_haf_data_accounts_body_operations (Job ID: 2368172)

**Status:** TIMEOUT (1 hour limit exceeded)

**Root Cause:** Same as prepare_haf_data - redundant local disk copy before NFS push.

**Evidence:**
```
[cache-manager] Relaxing pgdata permissions for caching
[cache-manager] Caching locally: /cache/haf_1892ff8a074a9438331a208934b006db1601fc82
Warning: Failed to push to NFS cache
ERROR: Job failed: execution took longer than 1h0m0s seconds
```

**Relation to CI Changes:** Same issue - cache-manager.sh was wasting time on unnecessary local disk copy.

**Fix Applied:** Same fix as prepare_haf_data - skip redundant local copy, prefer NFS.

---

### 3. haf_system_tests_mirrornet (Job ID: 2368176)

**Status:** FAILED

**Root Cause:** Missing blockchain directory in snapshot structure. The test expects a `blockchain` directory at `snapshot/../../blockchain` but it doesn't exist.

**Evidence:**
```
BlockLogError: /cache/snapshots_pipeline_141702/5m_mirrornet/snapshot/snapshot/../../blockchain is not a directory as required
```

**Relation to CI Changes:** The snapshot structure changed. Previously the mirrornet block_log was located differently. This may be related to our block_log path reorganization where we moved to `/cache/blockchain/block_log_5m`.

**Fix Required:**
1. Review `dump_snapshot_5m_mirrornet` job output structure
2. Ensure blockchain directory is included relative to snapshot
3. May need to adjust snapshot creation to include blockchain data or adjust test path expectations

---

### 4. replay_with_app (Job ID: 2368190)

**Status:** FAILED

**Root Cause:** Missing block_log at `/blockchain/block_log_5m/block_log`

**Evidence:**
```
export BLOCK_LOG_SOURCE_DIR_5M=/blockchain/block_log_5m
test -e /blockchain/block_log_5m/block_log
container must have /blockchain directory mounted containing block_log with at least 5000000 first blocks
exit 1
```

**Relation to CI Changes:** The maintenance script has a hardcoded path `/blockchain/block_log_5m` that was reverted by a Claude worker (commit 1892ff8a). However, the CI runners don't have this path mounted - they have block_log at `/cache/blockchain/block_log_5m`.

**Fix Required:** The maintenance script sets `BLOCK_LOG_SOURCE_DIR_5M` unconditionally, overriding any CI variable. This was supposed to be fixed in commit e50db662 to use `${BLOCK_LOG_SOURCE_DIR_5M:-/cache/blockchain/block_log_5m}` but that was reverted.

---

### 5. replay_restarts_with_app (Job ID: 2368193)

**Status:** FAILED

**Root Cause:** Same as replay_with_app - missing block_log at hardcoded path.

**Evidence:** Identical to replay_with_app

**Relation to CI Changes:** Same - reverted block_log path fix.

---

### 6. replay_with_restart (Job ID: 2368194)

**Status:** FAILED

**Root Cause:** Same as above.

---

### 7. replay_block_log_pruned_with_app (Job ID: 2368192)

**Status:** FAILED

**Root Cause:** Same as above.

---

### 8. update_with_wrong_table_schema (Job ID: 2368195)

**Status:** FAILED

**Root Cause:** Same as above.

---

## balance_tracker Pipeline 141713 Failed Jobs

### 1. sync (Job ID: 2368512)

**Status:** FAILED

**Root Cause:** Debug `ls` command trying to list pgdata directory contents after chmod 700. After restoring pgdata permissions to mode 700 and ownership to 105:105, the CI runner user cannot access the directory.

**Evidence:**
```
Restoring pgdata permissions to mode 700
total 16
drwxr-xr-x  4   105   105 4096 Dec  8 09:18 .
drwxr-xr-x  5 hived hived 4096 Dec  8 11:18 ..
drwx------ 19   105   105 4096 Dec  8 09:24 pgdata
drwx------  3   105   105 4096 Dec  8 09:18 tablespace
ls: cannot open directory '/builds/hive/balance_tracker/2368512/datadir/haf_db_store/pgdata/': Permission denied
```

**Relation to CI Changes:** We added pgdata permission restoration after copy_datadir.sh, but also added a debug `ls -la "${PGDATA_PATH}/"` command that fails because the CI runner user (hived) cannot read inside a directory with mode 700 owned by uid 105.

**Fix Required:** Remove the `ls -la "${PGDATA_PATH}/"` debug command - only list the parent directory, not inside pgdata. This was fixed in our latest commit (7805033a).

---

## reputation_tracker Pipeline 141690 Failed Jobs

### 1. sync (Job ID: 2368523)

**Status:** TIMEOUT

**Root Cause:** Same issue as HAF jobs - redundant local disk copy before NFS push. The sync completed successfully but the after_script timed out while doing unnecessary local cache copy.

**Evidence:**
```
Pushing sync data to NFS cache: haf_sync/e50db662040edc47eefdb103ca2f48414fe75046_45bbe249
[cache-manager] Caching locally: /cache/haf_sync_e50db662040edc47eefdb103ca2f48414fe75046_45bbe249
Terminated
Warning: Failed to push to NFS cache
WARNING: after_script could not run to completion because the timeout was exceeded
```

Note: The "Caching locally" message indicates the redundant copy operation was running when the timeout hit.

**Relation to CI Changes:** Same cache-manager.sh design flaw - doing local cache copy BEFORE NFS push.

**Fix Applied:** Same fix - skip redundant local copy, prefer NFS, use symlink for local reference.

---

## HAfAH Pipeline 141726 Failed Jobs

### 1-7. All test jobs (postgrest_pattern_tests, new_style_postgrest_pattern_tests, postgrest_comparison_tests, etc.)

**Status:** FAILED (237-301 test failures)

**Root Cause:** PostgreSQL tablespace directory missing. The HAF container tried to start with cached pgdata, but the tablespace subdirectory doesn't exist.

**Evidence:**
```
psql: error: connection to server on socket "/var/run/postgresql/.s.PGSQL.5432" failed:
FATAL:  database "haf_block_log" does not exist
DETAIL:  The database subdirectory "pg_tblspc/16396/PG_17_202406281/16397" is missing.
```

**Relation to CI Changes:** This is a critical issue with how we cache pgdata. PostgreSQL stores tablespaces as symlinks in `pg_tblspc/` directory that point to actual data. When we copy with `cp -r` or `rsync`, the symlink targets may not exist or the symlinks themselves may not be preserved correctly.

The tablespace directory structure:
- `pgdata/pg_tblspc/16396` -> symlink to tablespace directory
- The actual tablespace data at `/home/hived/datadir/haf_db_store/tablespace`

When caching, we need to:
1. Preserve the symlink correctly
2. Ensure the tablespace directory is also copied and accessible
3. Fix symlink targets if they point to absolute paths

**Fix Required:**
1. Review how tablespaces are cached - they use symlinks that may have absolute paths
2. Ensure `cp -a` preserves symlinks correctly (or recreate them)
3. The tablespace directory at `haf_db_store/tablespace` must be accessible with correct structure
4. May need to modify cache-manager.sh to handle PostgreSQL tablespace symlinks specially

---

## Summary of Root Causes

| Issue | Affected Jobs | Root Cause | Our Change |
|-------|--------------|------------|------------|
| **Redundant local copy** | prepare_haf_data, replay_filtered, rt/sync | cache-manager.sh does `cp -a` to local cache BEFORE NFS push, wasting 5+ minutes | NFS caching feature design flaw |
| Missing block_log | 5 replay jobs | Maintenance scripts hardcode `/blockchain/block_log_5m` | Reverted block_log path fix |
| Debug ls fails | bt/sync | `ls` inside pgdata after chmod 700 | pgdata permission restoration |
| Mirrornet blockchain | haf_system_tests_mirrornet | Snapshot doesn't include blockchain dir | Block_log path changes |
| Tablespace missing | All HAfAH tests | pg_tblspc symlinks not preserved in cache | Cache copy method (cp -r vs cp -a) |

## Recommended Fixes (Priority Order)

1. **[CRITICAL] Fix redundant local cache copy** - **FIXED in cache-manager.sh**
   - Problem: `cmd_put` was copying ~10GB to local cache BEFORE NFS push
   - This consumed remaining time after replay, causing timeouts
   - Fix: Skip local copy when NFS available, use symlink instead
   - Affects: prepare_haf_data, replay_filtered, rt/sync, bt/sync

2. **[CRITICAL] Fix PostgreSQL tablespace caching** - HAfAH tests can't run without this
   - Preserve symlinks when copying pgdata
   - May need to convert absolute symlinks to relative
   - Ensure tablespace directory structure is intact

3. **[HIGH] Remove debug ls inside pgdata** - Already fixed in 7805033a
   - Don't try to list pgdata contents after chmod 700

4. **[HIGH] Re-apply block_log path fix** - Un-revert e50db662
   - Maintenance scripts need `${BLOCK_LOG_SOURCE_DIR_5M:-/cache/blockchain/block_log_5m}`

5. **[LOW] Fix mirrornet snapshot structure** - Include blockchain directory
   - Review dump_snapshot_5m_mirrornet job

## NFS Performance Analysis

### Root Cause: NFS File Metadata Overhead

Testing revealed that the slow NFS performance was NOT due to Docker overhead, but due to **NFS file metadata operations**:

| Test | Speed |
|------|-------|
| Sequential write (dd single file) | 969 MB/s |
| cp -a (19GB, 1844 files) | 257 MB/s |
| tar pipe (streaming) | 260 MB/s |
| **tar archive (single file)** | **760 MB/s** |

**Key Insight**: Writing a single large file to NFS is 3x faster than writing many files because each file requires multiple NFS round-trips for metadata (OPEN, SETATTR, CLOSE).

### Solution: Store Cache as Tar Archives

Changed cache-manager.sh to store caches as single tar archive files on NFS:
- **PUT**: Creates `cache.tar` on NFS (single file = fast write)
- **GET**: Extracts tar to local disk (reading single file from NFS = fast)

**Performance Results (19GB, 1844 files):**
- PUT (tar archive): **25s** vs 74s (3x faster)
- GET (tar extract): **13s** vs 74s (5.7x faster)
- **Total: 38s vs 148s = 3.9x improvement**

## Code Changes Made

### cache-manager.sh (haf/scripts/ci-helpers/cache-manager.sh)

**Key Changes:**

1. **PUT now creates tar archive on NFS:**
```bash
# Write tar archive directly to NFS (single file = fast)
tar cf '${NFS_TAR_FILE}.tmp' -C '$local_source' .
mv '${NFS_TAR_FILE}.tmp' '$NFS_TAR_FILE'
```

2. **GET extracts tar archive from NFS:**
```bash
# Extract tar archive to local (fast: reading single file from NFS)
tar xf '$NFS_TAR_FILE' -C '$local_dest'
```

3. **Backward compatible with legacy directory format:**
```bash
# Check for tar archive first (new format), then directory (legacy format)
if [[ -f "$NFS_TAR_FILE" ]]; then
    use_tar=true
elif [[ -d "$NFS_CACHE_DIR" ]]; then
    # Legacy directory format
fi
```

4. **Local cache uses symlinks instead of copies:**
```bash
# Create local symlink to source for future local hits (instant, no copy)
ln -sf "$local_source" "$LOCAL_CACHE_DIR"
```

### copy_datadir.sh (haf/hive/scripts/copy_datadir.sh)

Modified to handle missing blockchain directory in cache by creating symlinks to shared block_log:

```bash
# Handle blockchain directory - may be excluded from cache for efficiency
if [[ -d "${DATA_SOURCE}/datadir/blockchain" ]]; then
    sudo chmod -R a+w "${DATA_SOURCE}/datadir/blockchain"
    ls -al "${DATA_SOURCE}/datadir/blockchain"
elif [[ -d "${SHARED_BLOCK_LOG_DIR}" ]]; then
    # Blockchain not in cache - create symlinks to shared block_log
    echo "Blockchain not in cache, linking to shared block_log at ${SHARED_BLOCK_LOG_DIR}"
    sudo -Enu hived mkdir -p "${DATADIR}/blockchain"
    for block_file in "${SHARED_BLOCK_LOG_DIR}"/block_log* ; do
        sudo -Enu hived ln -sf "$block_file" "${DATADIR}/blockchain/${local_name}"
    done
fi
```

Default shared block_log location: `/cache/blockchain/block_log_5m` (configurable via `SHARED_BLOCK_LOG_DIR`)
