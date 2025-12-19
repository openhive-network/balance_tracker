# Escrow Operations Mock Data

This fixture tests Balance Tracker's handling of escrow agreements.

## Operation Types

| ID | Operation | Virtual | Description |
|----|-----------|---------|-------------|
| 27 | `escrow_transfer_operation` | No | Creates escrow with funds and agent fee |
| 28 | `escrow_dispute_operation` | No | Raises a dispute (only agent can release after) |
| 29 | `escrow_release_operation` | No | Releases funds from escrow (partial or full) |
| 89 | `escrow_approved_operation` | Yes | Escrow approved (fee paid to agent) |
| 90 | `escrow_rejected_operation` | Yes | Escrow rejected (funds/fee returned to sender) |

**Note:** The non-virtual `escrow_approve_operation` (31) is NOT tracked by Balance Tracker.
Only the virtual `escrow_approved_operation` (89) triggers state changes.

## Escrow Lifecycle

1. **Create:** Sender creates escrow with `escrow_transfer_operation`
   - Funds (HIVE/HBD) locked in escrow
   - Agent fee held separately (in `escrow_fees` table)
2. **Approval:** Both agent AND receiver must approve before ratification deadline
   - `escrow_approved_operation` fires when fully approved
   - Fee is paid to agent (deleted from `escrow_fees`)
   - `to_approved` flag set to TRUE
3. **Release:** Funds released via `escrow_release_operation`
   - Can be partial or full
   - When fully released (amounts = 0), escrow is deleted
4. **Dispute:** Either party can raise dispute via `escrow_dispute_operation`
   - `disputed` flag set to TRUE
   - Only agent can release funds after dispute
5. **Rejection:** `escrow_rejected_operation` cancels escrow before approval
   - All funds and fee returned to sender
   - Escrow and fee records deleted

## Test Scenarios

All scenarios use: **from=cvk, to=fnait, agent=kush**

### (A) Escrow 90003001 — Transfer only
**Block 90000031:** Create escrow - 1000 HIVE, fee: 40 HIVE

**Result:** Remains pending (no approvals, no releases)
- escrow_state: 1000 HIVE, to_approved=FALSE, disputed=FALSE
- escrow_fees: 40 HIVE (pending)

---

### (B) Escrow 90003002 — Transfer → Approved → Dispute → Partial Release
**Block 90000032:** Create escrow - 2000 HIVE, fee: 20 HIVE
**Block 90000038:** Approved (fee deleted, paid to agent)
**Block 90000039:** Disputed by fnait
**Block 90000040:** Release 300 HIVE to fnait

**Result:** Active with remaining funds
- escrow_state: 1700 HIVE (2000-300), to_approved=TRUE, disputed=TRUE
- escrow_fees: (deleted on approval)

---

### (C) Escrow 90003003 — Transfer → Dispute → Rejected
**Block 90000033:** Create escrow - 1500 HIVE, fee: 10 HIVE
**Block 90000041:** Disputed by fnait
**Block 90000042:** Rejected

**Result:** DELETED
- escrow_state: (deleted)
- escrow_fees: (deleted)

---

### (D) Escrow 90003004 — Transfer → Fully Released (no approval)
**Block 90000034:** Create escrow - 500 HIVE, fee: 5 HIVE
**Block 90000043:** Release 100 HIVE
**Block 90000044:** Release 400 HIVE (total: 500, fully released)

**Result:** DELETED (fully released)
- escrow_state: (deleted, amounts zeroed)
- escrow_fees: 5 HIVE (still pending - never approved)

---

### (E) Escrow 90003005 — Mixed HIVE+HBD → Approved → Partial Release
**Block 90000035:** Create escrow - 1000 HIVE + 500 HBD, fee: 30 HIVE
**Block 90000036:** Approved (fee deleted, paid to agent)
**Block 90000046:** Release 500 HIVE + 200 HBD

**Result:** Active with remaining funds
- escrow_state: 500 HIVE, 300 HBD, to_approved=TRUE, disputed=FALSE
- escrow_fees: (deleted on approval)

---

### (F) Escrow 90003006 — Transfer → Dispute (never approved)
**Block 90000037:** Create escrow - 800 HIVE, fee: 15 HIVE
**Block 90000045:** Disputed by fnait

**Result:** Disputed but still pending approval
- escrow_state: 800 HIVE, to_approved=FALSE, disputed=TRUE
- escrow_fees: 15 HIVE (pending)

---

## Expected Final State

### escrow_state table (4 active escrows)

| Escrow ID | HIVE | HBD | Approved | Disputed |
|-----------|------|-----|----------|----------|
| 90003001 | 1000 | 0 | FALSE | FALSE |
| 90003002 | 1700 | 0 | TRUE | TRUE |
| 90003005 | 500 | 300 | TRUE | FALSE |
| 90003006 | 800 | 0 | FALSE | TRUE |

### escrow_fees table (3 pending fees)

| Escrow ID | Fee (HIVE) | Status |
|-----------|------------|--------|
| 90003001 | 40 | Not approved |
| 90003004 | 5 | Escrow fully released, fee still pending |
| 90003006 | 15 | Not approved, disputed |

### Aggregate Totals for account cvk

| Metric | Value |
|--------|-------|
| Total HIVE in escrow | 1000 + 1700 + 500 + 800 = 4000 |
| Total HBD in escrow | 300 |
| Active escrow count | 4 |
| Pending fees (HIVE) | 40 + 5 + 15 = 60 |

## Processing Notes

- **Squashing pattern:** When escrow is created AND fully released within the same block range, the escrow row is never inserted (optimization)
- **Fee lifecycle:** Fees are deleted only on approval (paid to agent) or rejection (returned to sender)
- **Dispute flag:** Once set to TRUE, cannot be unset (OR logic in upsert)
- **Approval flag:** Once set to TRUE, cannot be unset (OR logic in upsert)
