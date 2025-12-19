# Delegations Mock Data

This fixture tests Balance Tracker's handling of vesting share delegations.

## Operation Types

| ID | Operation | Virtual |
|----|-----------|---------|
| 40 | `delegate_vesting_shares_operation` | No |
| 41 | `account_create_with_delegation_operation` | No |
| 62 | `return_vesting_delegation_operation` | Yes |

## Test Scenarios

### Initial Balances
```
blocktrades: delegated_vests: 0, received_vests: 0
gtg:         delegated_vests: 0, received_vests: 0
```

### 1. Completed delegation cancellation
**Block 90000023:**
- Delegate: blocktrades → gtg, 10000 VESTS
- Cancel: blocktrades → gtg, 0 VESTS
- Return: blocktrades receives 10000 VESTS back

### 2. Change delegation amount
**Block 90000024:**
- Delegate: blocktrades → gtg, 10000 VESTS

**Block 90000025:**
- Update: blocktrades → gtg, 100000 VESTS (increases delegation)

### 3. Cancelled but not yet completed (5-day return delay)
**Block 90000026:**
- Delegate: gtg → blocktrades, 10000 VESTS

**Block 90000027:**
- Cancel: gtg → blocktrades, 0 VESTS
- Note: Return not yet processed (delegation still counts as "delegated")

### Expected Final Balances
```
blocktrades: delegated_vests: 100000, received_vests: 0
gtg:         delegated_vests: 10000,  received_vests: 100000
```

## Notes

- Delegation returns have a 5-day delay in the real blockchain
- `return_vesting_delegation_operation` is a virtual operation that fires when the delay completes
- Until the return operation, the delegator's `delegated_vests` still includes the cancelled amount
