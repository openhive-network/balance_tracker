# Savings Operations Mock Data

This fixture tests Balance Tracker's handling of savings-related operations.

## Operation Types

| ID | Operation | Virtual |
|----|-----------|---------|
| 32 | `transfer_to_savings_operation` | No |
| 33 | `transfer_from_savings_operation` | No |
| 34 | `cancel_transfer_from_savings_operation` | No |
| 55 | `interest_operation` | Yes |
| 59 | `fill_transfer_from_savings_operation` | Yes |

## Test Scenarios

### Initial Balance (blocktrades)
```json
{
    "hbd_balance": 77246982,
    "hive_balance": 29594875,
    "hbd_savings": 0,
    "hive_savings": 0
}
```

### Block 90000001
- `transfer_to_savings_operation` (blocktrades, HBD, 10000)
- `transfer_to_savings_operation` (blocktrades, HIVE, 10000)
- `interest_operation` (blocktrades, HBD, 100)
- `transfer_from_savings_operation` (blocktrades, HBD, 1000, id:123)
- `transfer_from_savings_operation` (blocktrades, HBD, 2000, id:321)
- `cancel_transfer_from_savings_operation` (blocktrades, id:123)
- `fill_transfer_from_savings_operation` (blocktrades, id:321)

### Block 90000002
- `transfer_from_savings_operation` (blocktrades, HIVE, 3000, id:123)

### Block 90000003
- `transfer_from_savings_operation` (blocktrades, HIVE, 4000, id:321)

### Block 90000004
- `cancel_transfer_from_savings_operation` (blocktrades, id:123)

### Block 90000005
- `fill_transfer_from_savings_operation` (blocktrades, id:321)

### Expected Final Balance (blocktrades)
```json
{
    "hbd_balance": 77238982,
    "hive_balance": 29588875,
    "hbd_savings": 8100,
    "hive_savings": 6000
}
```
