# Delayed Voting Mock Data

This fixture tests Balance Tracker's handling of delayed voting operations (HF24+).

## Operation Types

| ID | Operation | Virtual |
|----|-----------|---------|
| 56 | `fill_vesting_withdraw_operation` | Yes |
| 70 | `delayed_voting_operation` | Yes |
| 77 | `transfer_to_vesting_completed_operation` | Yes |

## Background

Hardfork 24 introduced a 30-day delay before newly powered-up VESTS gain full voting power.
This prevents vote manipulation by powering up just before a vote.

## Test Scenarios

### Initial Balances
```
blocktrades: HIVE 29594875, VESTS 8172549681957451, delayed_vests: 0
steem:       HIVE 29325310, VESTS 15636871956265,   delayed_vests: 0
```

### 1. Power up with immediate delayed_voting cancellation
**Block 90000028:**
- `transfer_to_vesting_completed_operation` (steem powers up 100 HIVE → 1000000 VESTS)
- `delayed_voting_operation` (steem, 1000000 VESTS) - cancels the delay immediately

Result: steem's delayed_vests = 0 (delay was cleared)

### 2. Power up with partial reduction via power down
**Block 90000029:**
- `transfer_to_vesting_completed_operation` (blocktrades powers up 10000 HIVE → 90404818220529 VESTS)

**Block 90000030:**
- `fill_vesting_withdraw_operation` (blocktrades withdraws 80404818220529 VESTS)

Result: blocktrades still has delayed_vests = 10000000000000 (partial delay remains)

### Expected Final Balances
```
blocktrades: HIVE 29593875, VESTS 8182549681957451, delayed_vests: 10000000000000
steem:       HIVE 29325210, VESTS 15636872956265,   delayed_vests: 0
```

## Notes

- `delayed_voting_operation` clears delayed vests after the 30-day period
- Power-downs reduce delayed_vests proportionally
- This mock data requires HF24 to be simulated (done by `update_irreversible_block()`)
