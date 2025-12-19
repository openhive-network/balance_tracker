# Block Headers Mock Data

This fixture provides mock block headers for the test block range.

## Block Range

Mock blocks use block numbers >= 90000000 to avoid conflicts with real blockchain data. The actual range is determined dynamically from the inserted fixture data.

## Purpose

The mock blocks provide:
- Block timestamps for time-based queries
- Witness/producer information
- Global blockchain state (supply, vesting, interest rate)

## Usage

These blocks are inserted first, before any operations. They establish the blockchain "structure" that operations reference.

## File: data.json

Contains an array of block headers with fields:
- `block_num` - Block number
- `hash` - Block hash (hex-encoded)
- `prev` - Previous block hash (hex-encoded)
- `created_at` - Block timestamp
- `producer_account` - Witness who produced the block
- `total_vesting_fund_hive` - Global vesting fund
- `total_vesting_shares` - Global vesting shares
- And other blockchain state fields...
