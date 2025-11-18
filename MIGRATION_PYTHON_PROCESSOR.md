# Migration to Python Block Processor

## Overview

The Balance Tracker block processing has been migrated from a SQL-based infinite loop to a Python-based processor. This change was necessary to support PostgreSQL deployments with statement timeout settings.

## What Changed

### Before
- Block processing used `CALL btracker_app.main('btracker_app', <limit>)` 
- This created a single, long-running SQL query that could run indefinitely
- Would fail on systems with statement timeouts configured

### After
- Block processing now uses `python3 process_blocks.py`
- Each iteration is a separate SQL transaction
- Compatible with statement timeout settings
- Better control and monitoring capabilities

## Migration Steps

### For Script Users

If you were calling `scripts/process_blocks.sh`, **no changes needed**. The script has been updated to use the Python processor automatically.

```bash
# This still works the same way
./scripts/process_blocks.sh --stop-at-block=1000000

# Or for continuous processing
./scripts/process_blocks.sh
```

### For Direct SQL Users

If you were calling the SQL procedure directly:

**Old method (deprecated):**
```sql
CALL btracker_app.main('btracker_app', 1000000);
```

**New method:**
```bash
python3 process_blocks.py --context=btracker_app --stop-at-block=1000000
```

## Python Script Usage

The new `process_blocks.py` script supports the following options:

```bash
python3 process_blocks.py --help

Options:
  --context TEXT           HAF context name (default: btracker_app)
  --schema TEXT            Database schema name (default: btracker_app)
  --stop-at-block INTEGER  Maximum block number to process (default: None)
  --host TEXT              PostgreSQL host (default: localhost)
  --port INTEGER           PostgreSQL port (default: 5432)
  --user TEXT              PostgreSQL user (default: btracker_owner)
  --database TEXT          PostgreSQL database (default: haf_block_log)
  --url TEXT               Full PostgreSQL connection URL (overrides other options)
```

### Examples

**Process blocks indefinitely:**
```bash
python3 process_blocks.py --host=localhost --user=btracker_owner
```

**Process up to block 5000000:**
```bash
python3 process_blocks.py --stop-at-block=5000000
```

**Use a connection URL:**
```bash
python3 process_blocks.py --url="postgresql://user@host:5432/haf_block_log"
```

**Custom schema:**
```bash
python3 process_blocks.py --schema=my_btracker --context=my_btracker
```

## Benefits

1. **Statement Timeout Compatible**: Each block range is processed in a separate transaction
2. **Better Monitoring**: Python logging provides clearer status messages
3. **Graceful Shutdown**: Ctrl+C properly stops processing at transaction boundaries
4. **More Control**: Command-line interface with standard argument parsing
5. **Easier Debugging**: Python stack traces are easier to read than SQL errors

## Backward Compatibility

The old SQL `main()` procedure is still available but deprecated. It will:
- Display a warning message when called
- Still function for backward compatibility
- Be removed in a future version

## Technical Details

### How It Works

1. The Python script connects to PostgreSQL
2. For each iteration:
   - Calls `hive.app_next_iteration()` to get the next block range
   - If blocks are available, calls `btracker_process_blocks()` to process them
   - Repeats (the next iteration will commit the work)
3. If no blocks are available, waits briefly and tries again
4. Continues until stopped or max block reached

### Transaction Management

- Each call to `app_next_iteration` commits at the beginning of its execution (HAF requirement)
- This commit includes both HAF's internal state updates AND the application's work from the previous iteration
- No explicit commits are made by the application code between iterations
- This ensures atomic updates - HAF's state and the application's state are always synchronized
- On error or interrupt, the current transaction is rolled back, and processing can resume from the last successful iteration

### Dependencies

The script requires:
- Python 3.6+
- psycopg2 library (already a project dependency)

No new dependencies were added.

## Troubleshooting

### "Import psycopg2 could not be resolved"

Ensure psycopg2 is installed:
```bash
pip3 install psycopg2-binary
```

### Script doesn't have execute permissions

```bash
chmod +x process_blocks.py
```

### Connection refused

Check your PostgreSQL connection parameters:
```bash
python3 process_blocks.py --host=localhost --port=5432 --user=btracker_owner --database=haf_block_log
```

### "continueProcessing() returned False"

This is normal. The processing was stopped by calling `stopProcessing()` function or by modifying the `btracker_app_status` table. The processor will exit cleanly.

## Support

For issues or questions, please file an issue in the project repository.
