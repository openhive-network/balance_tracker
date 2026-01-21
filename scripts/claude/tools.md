# Utility Scripts

These scripts help with common development and debugging tasks. **Use them automatically when the task matches their purpose.**

## Available Tools

### check-balance-tracker-pipeline.sh

**Location**: `scripts/claude/tools/check-balance-tracker-pipeline.sh`

**Use when**: User asks to analyze failed CI/CD pipelines, check pipeline status, or debug test failures in Balance Tracker.

**Usage**:
```bash
# Check latest pipeline for a branch
./scripts/claude/tools/check-balance-tracker-pipeline.sh feature/my-branch

# Check specific pipeline by ID
./scripts/claude/tools/check-balance-tracker-pipeline.sh 141713
```

**What it does**:
- Fetches pipeline status from GitLab API (project ID 330)
- Shows summary of job statuses (success/failed/running)
- Lists key jobs: prepare_haf_image, prepare_haf_data, sync, sync_with_mock_data, regression-test, performance-test, pattern-test
- For failed jobs: extracts relevant error lines from logs with job-specific filtering
- Shows running and canceled jobs

**Output sections**:
- Pipeline summary (branch, SHA, status, URL)
- Job status counts
- Key jobs status (OK/FAIL/RUN)
- Failed jobs with extracted error context
- Running jobs with runner info

---

### run_sync_test.sh

**Location**: `scripts/claude/tools/run_sync_test.sh`

**Use when**: User wants to test sync performance, benchmark block processing, or measure optimization impact.

**Usage**:
```bash
# Default: sync to 5M blocks
./scripts/claude/tools/run_sync_test.sh

# Custom target block
./scripts/claude/tools/run_sync_test.sh 1000000

# Custom target and host
./scripts/claude/tools/run_sync_test.sh 5000000 172.17.0.2

# Full: target, host, log file
./scripts/claude/tools/run_sync_test.sh 5000000 172.17.0.2 my_test.log
```

**What it does**:
- Starts `process_blocks.sh` with specified target block
- Monitors progress in real-time
- Automatically stops when target block reached
- Calculates total processing time, blocks processed, average time per 10K blocks
- Compares against baseline performance metrics

**Output includes**:
- Total processing time in seconds and minutes
- Block ranges processed count
- Performance comparison against baseline (167.55s) and previous bests

---

### job_failure_analysis.md

**Location**: `scripts/claude/tools/job_failure_analysis.md`

**Use when**: Investigating CI/CD failures, understanding cache-manager issues, or debugging NFS/tablespace problems.

**What it contains**:
- Detailed analysis of common pipeline failures across HAF, btracker, reptracker, hafah
- Root cause explanations for timeout, missing block_log, permission, and tablespace issues
- Code changes made to fix specific problems
- NFS performance analysis and optimization findings
- Recommended fixes with priority ordering

**Key topics covered**:
- Cache-manager redundant local copy issue (causes timeouts)
- PostgreSQL tablespace caching with symlinks
- Block_log path configuration
- pgdata permission restoration
- NFS file metadata overhead and tar archive optimization

---

### block_log_reorganization_plan.md

**Location**: `scripts/claude/tools/block_log_reorganization_plan.md`

**Use when**: Planning or implementing changes to block_log directory structure on CI builders.

**What it contains**:
- Current state of block_log files across builders
- Problem description (mixed monolithic and split formats)
- Proposed directory reorganization
- Step-by-step implementation commands
- Affected builders list

---

## When to Use These Tools

| Task | Script to Use |
|------|---------------|
| Analyze failed CI/CD pipeline | `check-balance-tracker-pipeline.sh` |
| Check pipeline job status | `check-balance-tracker-pipeline.sh` |
| Debug test failures | `check-balance-tracker-pipeline.sh` |
| Test sync performance | `run_sync_test.sh` |
| Benchmark optimizations | `run_sync_test.sh` |
| Understand CI failure patterns | `job_failure_analysis.md` |
| Debug cache/NFS issues | `job_failure_analysis.md` |
| Block_log directory changes | `block_log_reorganization_plan.md` |

## Expansion Rules

When adding new utility scripts:
1. Add script to `scripts/claude/tools/`
2. Document it here with: Location, Use when, Usage, What it does
3. Add to the "When to Use" table
4. Include example commands with common use cases

## Note for Other Apps

These scripts serve as templates for other HAF apps (HAFBE, reptracker, hafah). When setting up documentation for those apps:
1. Copy relevant scripts
2. Update PROJECT_ID in pipeline checker (330 → target project ID)
3. Update key job names to match target project's CI configuration
4. Adjust baseline performance numbers for benchmarks
