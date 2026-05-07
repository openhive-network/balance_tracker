# Savings Pending Mock Data

This fixture adds an in-flight savings withdrawal scenario for `steem`.

## Test Scenario

### Block 90000052
- `transfer_to_savings_operation` (steem, HBD, 1000)
- `transfer_to_savings_operation` (steem, HIVE, 2000)

### Block 90000053
- `transfer_from_savings_operation` (steem, HBD, 500, id: 9001)
- `transfer_from_savings_operation` (steem, HIVE, 700, id: 9002)

There are no matching `cancel_transfer_from_savings_operation` or
`fill_transfer_from_savings_operation` entries, so both withdrawals remain
pending.

## Expected Account Balance Effects

For `steem`:

```json
{
  "hbd_balance": 479002,
  "hive_balance": 29323210,
  "hbd_savings": 500,
  "hive_savings": 1300,
  "savings_pending_amount_hbd": 500,
  "savings_pending_amount_hive": 700,
  "savings_withdraw_requests": 2
}
```

This catches regressions where pending savings withdrawals are returned as
negative values.
