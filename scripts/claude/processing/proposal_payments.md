# Proposal Payments Processing

Per-proposal record of what the Decentralized Hive Fund (DHF) has paid.

## Overview

`process_block_range_proposal_payments()` reads every `proposal_pay_operation` (virtual,
one per paid proposal at each hourly DHF maintenance) in the batch and records it per
`proposal_id`. `process_balances` already moves the HBD (receiver +payment, payer
-payment) but the balance history cannot tell which of a receiver's proposals a payment
was for.

**File**: [db/process_proposal_payments.sql](../../../db/process_proposal_payments.sql)

## Key Functions

| Function | Purpose | Location |
|----------|---------|----------|
| `process_block_range_proposal_payments(_from, _to)` | Records the range's payments | `db/process_proposal_payments.sql` |
| `get_proposal_payment_event(body)` | `proposal_id`, `receiver`, HBD amount of one operation | `backend/operation_parsers/proposal_payments.sql` |

## Tables Updated

| Table | Read/Write | Purpose |
|-------|------------|---------|
| `proposal_payment_totals` | RW | Per proposal: receiver, `total_paid` (HBD satoshis), `payment_count`, first and last payment block/time |
| `proposal_payments` | W | Every payment: `proposal_id`, `payment_seq_no` (1..n per proposal), amount, `source_op` |

Both are registered with HAF, so a fork rollback reverts them together.

## Invariants

- `payment_count` equals the highest `payment_seq_no` of the proposal. New payments are
  numbered from the `payment_count` read before the range, in `source_op` order, in the
  same statement that adds them to the totals. The history endpoint pages on
  `(proposal_id, payment_seq_no)` with `payment_count` as the row count.
- A proposal's receiver is fixed at creation (`update_proposal` cannot change it).
- The treasury pays in HBD only; the amount is stored without its NAI.

## Chain history

Every `proposal_pay_operation` in HAF carries `proposal_id`, from the first payment
(block 35,922,616, HF21, payer `steem.dao`) on: HAF stores the bodies hived's replay
emits. Checked on a full mainnet HAF at block 111,029,235: 820,984 operations, all with a
numeric `proposal_id`, 195 distinct proposals, one NAI (HBD), one receiver per proposal.

## Why per-payment rows

The history endpoint could read `hafd.operations` by `op_type_id` instead, but
`proposal_id` lives only in the operation body, so one proposal's history would scan
every payment of the chain (~820k rows). `proposal_payments` is that many narrow rows,
read by primary key.

## Existing installs

Like `balance_totals`, the tables are created only when the context is created
(`db/btracker_app.sql` returns early when it exists): an install that predates them
needs a fresh install and sync.
