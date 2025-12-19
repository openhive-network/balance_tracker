# Block Log Directory Reorganization Plan

## Current State

The 5M block_log files exist in multiple locations with mixed formats:

### `/blockchain/block_log_5m/` (static, root-owned)
- Split parts only: `block_log_part.0001` through `block_log_part.0005`
- Includes `.artifacts` files
- No monolithic `block_log`
- This is causing test failures for jobs that expect monolithic format

### `/cache/blockchain/block_log_5m/` (all builders)
- Contains BOTH formats in same directory:
  - Monolithic: `block_log` (1.6GB)
  - Split: `block_log_part.0001` through `block_log_part.0005`
- Files are hard-linked across builders to save space
- Currently used by HAF CI after our fix

## Problem

Having both formats in the same directory is confusing and error-prone. Tests that need one format might accidentally pick up the other.

## Proposed Reorganization

Create separate directories with clear naming:

```
/cache/blockchain/
├── block_log_5m/           # Monolithic format only
│   └── block_log           # Single 1.6GB file
│
└── block_log_5m_split/     # Split format only
    ├── block_log_part.0001
    ├── block_log_part.0002
    ├── block_log_part.0003
    ├── block_log_part.0004
    └── block_log_part.0005
```

## Implementation Steps

1. On each builder (8, 9, 10):
   ```bash
   # Create new split directory
   mkdir -p /cache/blockchain/block_log_5m_split

   # Move split parts to new directory (preserve hard links)
   mv /cache/blockchain/block_log_5m/block_log_part.* /cache/blockchain/block_log_5m_split/

   # Verify monolithic file remains
   ls -la /cache/blockchain/block_log_5m/
   ```

2. Update CI variables in HAF `.gitlab-ci.yml`:
   ```yaml
   BLOCK_LOG_SOURCE_DIR_5M: /cache/blockchain/block_log_5m           # monolithic
   BLOCK_LOG_SOURCE_DIR_5M_SPLIT: /cache/blockchain/block_log_5m_split  # split parts
   ```

3. Update any jobs that specifically need split format to use the new variable.

## Affected Builders

- hive-builder-8 (32-Core 5950X, 128G)
- hive-builder-9 (32-Core 5950X, 128G)
- hive-builder-10 (NFS server)

## Notes

- Hard links should be preserved when moving files within same filesystem
- The `/blockchain/block_log_5m/` static directory can remain as-is for now (or be updated separately)
- Consider adding artifacts files to split directory if needed by some tests
