# Multi-Balance Operations Mock Data (balance_seq_no Regression Test)

This fixture tests a specific edge case where a single operation produces **multiple identical balance changes** for the same account and asset.

## The Bug This Tests

A bug was identified in `process_balances.sql` where the `ROW_NUMBER()` window function ordered by the **input delta** instead of the **computed running balance**.

When an operation produces two balance changes with:
- Same account
- Same asset (nai)
- Same source_op
- **Same delta amount**

The ORDER BY becomes non-deterministic (tie), potentially selecting the wrong row as "last". This causes:
1. Wrong `balance_seq_no` stored in `current_account_balances`
2. Duplicate `balance_seq_no` values in subsequent block ranges
3. Unique index creation failure when transitioning to LIVE mode

## Test Scenario

**Accounts:** from=smooth, to=arhag, agent=freedom

`smooth` is chosen because it has existing HIVE balance in the real chain (blocks 1-5m) and is not used in other pattern tests.

**Block 90000050:** Create escrow with **equal amounts**
- hive_amount: 1000 milliHIVE (1 HIVE)
- fee: 1000 milliHIVE (1 HIVE) ← **Same as hive_amount!**

**Block 90000051:** escrow_rejected_operation
- Returns +1000 (hive_amount) to `smooth`
- Returns +1000 (fee) to `smooth`
- **Both deltas are identical = TIE condition**

## Expected Behavior

With the fix, the `ROW_NUMBER()` orders by the **computed running balance** (which is always different: X vs X+1000), not the input delta. This ensures deterministic selection of the correct "last" row.

The balance history for account `smooth` should have sequential, non-duplicate `balance_seq_no` values.

## Real-World Example

This bug was triggered by operation 98156611352161882 at block 22853867:
```json
{
  "type": "escrow_rejected_operation",
  "from": "talhasch",
  "hive_amount": {"amount": "1"},
  "fee": {"amount": "1"}
}
```

Both amounts were 1 milliHIVE, creating the tie condition.
