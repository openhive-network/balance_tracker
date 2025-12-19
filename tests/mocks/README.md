# Balance Tracker Mock Data System

This directory contains mock blockchain data for testing Balance Tracker without a live blockchain or HAF replay.

## Overview

The mock system allows testing Balance Tracker's operation processing by:
1. Inserting fake block headers into HAF's `hafd.blocks` table
2. Inserting fake operations into HAF's `hafd.operations` table
3. Configuring HAF state to recognize these blocks as processable

This enables fast, repeatable testing of specific operation scenarios without syncing from the real blockchain.

## Directory Structure

```
tests/mocks/
├── README.md                 # This file
├── install_mock_data.sh      # Main installation script
├── sql/
│   ├── types.sql             # PostgreSQL type definitions for JSON parsing
│   ├── insert_blocks.sql     # Function to insert mock block headers
│   ├── insert_operations.sql # Function to insert mock operations
│   └── update_haf_state.sql  # Functions to configure HAF state
└── fixtures/
    ├── blocks/               # Mock block headers (required)
    ├── savings/              # Savings operations test data
    ├── rewards/              # Rewards operations test data
    ├── recurrent_transfers/  # Recurrent transfer test data
    ├── delegations/          # Vesting delegation test data
    ├── delays/               # Delayed voting (HF24) test data
    └── escrow/               # Escrow operations test data
```

## How It Works

### 1. Block Range Convention

Mock blocks use block numbers **>= 90,000,000** to avoid conflicts with real blockchain data. The actual range is determined dynamically from the inserted fixture data.

### 2. Installation Process

The `install_mock_data.sh` script performs these steps:

1. **Load SQL functions** - Installs helper functions into `btracker_backend` schema
2. **Insert block headers** - Populates `hafd.blocks` from `fixtures/blocks/data.json`
3. **Insert operations** - Populates `hafd.operations` from each fixture category
4. **Update HAF state** - Configures HAF to process the mock block range:
   - Sets `hafd.hive_state.consistent_block` to the last mock block
   - Sets `hafd.contexts.current_block_num` and `irreversible_block` to the first mock block
   - Inserts HF24 hardfork entry (required for delayed voting tests)

### 3. Processing Flow

After mock data is installed:

```
hafd.blocks          → Contains mock block headers
hafd.operations      → Contains mock operations (binary format)
hafd.hive_state      → consistent_block = last mock block
hafd.contexts        → current_block_num = first mock block
                       irreversible_block = first mock block
```

When `process_blocks.sh` runs, Balance Tracker:
1. Detects blocks to process (from current_block_num to consistent_block)
2. Reads operations from `hafd.operations`
3. Processes them through the normal block processing pipeline
4. Updates balance tables with results

## Fixture Format

### Block Headers (`fixtures/blocks/data.json`)

```json
{
  "blocks": [
    {
      "block_num": 90000001,
      "hash": "abc123...",           // hex-encoded
      "prev": "def456...",           // hex-encoded
      "created_at": "2025-01-01T00:00:00",
      "producer_account": "witness-name",
      "total_vesting_fund_hive": "1000000.000",
      "total_vesting_shares": "2000000.000000",
      ...
    }
  ]
}
```

### Operations (`fixtures/*/data.json`)

```json
{
  "operations": [
    {
      "block_num": 90000001,
      "op_type_id": 32,              // maps to hive.operation_types
      "op_pos": 0,                   // position in block
      "trx_in_block": 0,             // transaction index (-1 for virtual ops)
      "body": {
        "type": "transfer_to_savings_operation",
        "value": {
          "from": "alice",
          "to": "alice",
          "amount": {"nai": "@@000000021", "amount": "1000", "precision": 3},
          "memo": "saving for later"
        }
      }
    }
  ]
}
```

### Operation Type IDs

Common operation types used in fixtures:

| ID | Operation | Virtual |
|----|-----------|---------|
| 2 | `transfer_operation` | No |
| 4 | `transfer_to_vesting_operation` | No |
| 20 | `delegate_vesting_shares_operation` | No |
| 27 | `escrow_transfer_operation` | No |
| 32 | `transfer_to_savings_operation` | No |
| 33 | `transfer_from_savings_operation` | No |
| 49 | `claim_reward_balance_operation` | No |
| 51 | `recurrent_transfer_operation` | No |
| 52 | `author_reward_operation` | Yes |
| 53 | `curation_reward_operation` | Yes |
| 56 | `fill_transfer_from_savings_operation` | Yes |
| 64 | `return_vesting_delegation_operation` | Yes |
| 80 | `fill_recurrent_transfer_operation` | Yes |
| 89 | `escrow_approved_operation` | Yes |
| 90 | `escrow_rejected_operation` | Yes |

## Usage

### Prerequisites

1. HAF database running and accessible
2. Balance Tracker schema installed (`scripts/install_app.sh`)
3. Basic accounts exist in the database (blocktrades, gtg, steem, etc.)

### Running Locally

```bash
# Install mock data
./tests/mocks/install_mock_data.sh

# Process the mock blocks
./scripts/process_blocks.sh

# Run API tests against mock data
cd tests/tavern && pytest
```

### Running in Docker

```bash
# Using docker-compose
docker compose exec btracker install_mocks

# Or directly
docker run <image> install_mocks --host=haf --user=haf_admin
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | localhost | PostgreSQL hostname |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_USER` | haf_admin | PostgreSQL user |
| `POSTGRES_URL` | - | Full connection URL (overrides above) |

## Adding New Test Fixtures

### 1. Create a New Fixture Category

```bash
mkdir tests/mocks/fixtures/my_feature
```

### 2. Create data.json

Add operations following the JSON format above. Use block numbers in the 90,000,000+ range.

### 3. Create README.md

Document the test scenarios and expected results.

### 4. Update install_mock_data.sh

Add loading logic for your new fixture:

```bash
MY_FEATURE_DATA=$(cat "$FIXTURES_DIR/my_feature/data.json")
echo "  - My feature operations..."
psql "$POSTGRES_ACCESS" -v ON_ERROR_STOP=on \
    -c "SELECT btracker_backend.insert_mock_operations('$MY_FEATURE_DATA')"
```

### 5. Add Blocks If Needed

If your operations use new block numbers, add corresponding entries to `fixtures/blocks/data.json`.

## Troubleshooting

### "No mock blocks found" Error

The `update_irreversible_block()` function couldn't find any blocks >= 90,000,000 in `hafd.blocks`. Ensure block headers are inserted before calling this function.

### Operations Not Processing

Check that:
1. Block numbers in operations match existing blocks in `fixtures/blocks/data.json`
2. Operation type IDs are valid (check `hive.operation_types`)
3. HAF state is correctly configured (run `SELECT * FROM hafd.hive_state`)

### Account Not Found

Mock operations reference accounts that must exist in the HAF database. The standard HAF setup includes common accounts like `blocktrades`, `gtg`, `steem`. For custom accounts, you may need to insert them first.

## Technical Notes

### Dynamic Block Range Detection

The system uses `get_mock_block_range()` to query `hafd.blocks` for the actual min/max block numbers after insertion. This means:
- No hardcoded block ranges in scripts
- Adding new blocks only requires updating JSON fixtures
- The system adapts automatically to fixture changes

### Hardfork Simulation

HF24 is automatically inserted at `first_block + 1` because:
- Delayed voting tests require HF24 to be active
- The hardfork entry enables `delayed_voting_operation` processing
- Other hardforks can be added similarly if needed

### Operation ID Generation

Operations are inserted with IDs generated by `hafd.operation_id(block_num, op_type_id, op_pos)`. This matches HAF's internal ID scheme and ensures operations can be looked up correctly.
