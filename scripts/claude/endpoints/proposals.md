# Proposal Endpoints

What the Decentralized Hive Fund has paid each proposal, from `proposal_pay_operation`
(see [processing/proposal_payments.md](../processing/proposal_payments.md)).

## Endpoint List

| Endpoint | Purpose |
|----------|---------|
| `GET /proposals/payments?proposal-ids=0,233,340` | Totals for many proposals in one call |
| `GET /proposals/{proposal-id}/payments` | Paged payment history of one proposal |

## `GET /proposals/payments`

- `proposal-ids` (required): comma-separated non-negative ids, at most 1000. Anything
  else is a 400.
- One row per distinct id, ascending: `proposal_id`, `receiver`, `total_paid` (HBD amount
  object), `payment_count`, `first_payment_block/time`, `last_payment_block/time`.
- An id never paid (or not a proposal: btracker sees payments, not proposal creation)
  gets `total_paid` 0, `payment_count` 0 and nulls.
- Point reads of `proposal_payment_totals` (a few hundred rows) plus a receiver-name
  lookup per paid proposal. Short cache (2 s).

## `GET /proposals/{proposal-id}/payments`

- `page`, `page-size` (max 1000), `direction` as in the other history endpoints: page 1
  is the oldest, the default page the newest.
- Rows: `block_num`, `operation_id`, `amount` (HBD), `timestamp`.
- The row count is the totals row's `payment_count`; the page is a `payment_seq_no`
  window on `pk_proposal_payments` (`force_custom_plan`, like vesting history).

## Implementation

| Layer | File |
|-------|------|
| Endpoints | `endpoints/proposals/get_proposals_payments.sql`, `endpoints/proposals/get_proposal_payments.sql` |
| Backend | `backend/endpoint_helpers/get_proposal_payments/proposal_payments.sql` |
| Types | `endpoints/types/proposal_payments.sql` |
