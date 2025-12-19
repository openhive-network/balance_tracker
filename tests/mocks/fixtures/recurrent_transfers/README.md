# Recurrent Transfers Mock Data

This fixture tests Balance Tracker's handling of scheduled recurring transfers.

## Operation Types

| ID | Operation | Virtual |
|----|-----------|---------|
| 49 | `recurrent_transfer_operation` | No |
| 83 | `fill_recurrent_transfer_operation` | Yes |
| 84 | `failed_recurrent_transfer_operation` | Yes |

## Test Scenarios

### Initial Balances
```
blocktrades: HBD 77254982, HIVE 29604875
gtg:         HBD 3465,     HIVE 0
steem:       HBD 500001,   HIVE 29315310
```

### 1. Regular recurrent transfer with a fail at the end
**Block 90000010:**
- Create: blocktrades → gtg, HBD 10000, every 24h, 10 executions
- Fill: immediate execution
- Fail: insufficient funds simulation

### 2. Regular recurrent transfer with a fill at the end
**Block 90000011:**
- Create: blocktrades → steem, HIVE 10000, every 24h, 10 executions
- Fail: insufficient funds
- Fill: successful execution

### 3. Overwritten transfer with currency change (trigger date unchanged)
**Blocks 90000012-90000014:**
- Create: steem → blocktrades, HBD 10000
- Fill: execute
- Overwrite: change to HIVE 10000 (same trigger date)

### 4. Overwritten transfer with recurrence change (trigger date changes)
**Blocks 90000015-90000017:**
- Create: steem → gtg, HBD 10000, every 24h
- Fill: execute
- Overwrite: change to every 48h (trigger date updates)

### 5. Update of transfer (trigger date unchanged)
**Blocks 90000018-90000020:**
- Create: gtg → blocktrades, HBD 1000
- Fill: execute
- Update: same parameters (trigger date unchanged)

### 6. Cancel by setting amount to 0
**Blocks 90000021-90000022:**
- Create: gtg → steem, HBD 1000
- Cancel: set amount to 0

### 7. Remove when remaining_executions reaches 0
**Blocks 90000023-90000024:**
- Create: gtg → steem, HBD 1, 1 execution
- Fill: complete (removed after)

### 8. Second recurrent transfer with pair_id=1
**Blocks 90000047-90000048:**
- Create: gtg → blocktrades, HBD 10, pair_id=1
- Fill: execute

### Expected Final Balances
```
blocktrades: HBD 77255992, HIVE 29594875
gtg:         HBD 22454,    HIVE 0
steem:       HBD 480002,   HIVE 29325310
```
