# Rewards Operations Mock Data

This fixture tests Balance Tracker's handling of reward-related operations.

## Operation Types

| ID | Operation | Virtual |
|----|-----------|---------|
| 39 | `claim_reward_balance_operation` | No |
| 51 | `author_reward_operation` | Yes |
| 63 | `comment_benefactor_reward_operation` | Yes |

## Test Scenarios

### Initial Balance (blocktrades)
```json
{
    "hbd_balance": 77238982,
    "hive_balance": 29588875,
    "vesting_shares": "8172549681941451",
    "hbd_rewards": 0,
    "hive_rewards": 0,
    "vests_rewards": "0"
}
```

### Block 90000006
- `author_reward_operation` (blocktrades, hbd:10000, hive:10000, vests:10000)
- `comment_benefactor_reward_operation` (blocktrades, hbd:10000, hive:10000, vests:10000)
- `claim_reward_balance_operation` (blocktrades, hbd:15000, hive:15000, vests:15000)

### Block 90000007
- `author_reward_operation` (blocktrades, hbd:1000, hive:1000, vests:1000)

### Block 90000008
- `comment_benefactor_reward_operation` (blocktrades, hbd:1000, hive:1000, vests:1000)

### Block 90000009
- `claim_reward_balance_operation` (blocktrades, hbd:1000, hive:1000, vests:1000)

### Expected Final Balance (blocktrades)
```json
{
    "hbd_balance": 77254982,
    "hive_balance": 29604875,
    "vesting_shares": "8172549681957451",
    "hbd_rewards": 6000,
    "hive_rewards": 6000,
    "vests_rewards": "6000"
}
```
