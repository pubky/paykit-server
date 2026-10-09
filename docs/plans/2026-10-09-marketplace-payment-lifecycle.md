# Marketplace Payment Lifecycle Decision Ledger

Status: preparation, activation, void, and resolution slices authorized.

## Authority and baseline

- Authoritative product contract: [pubky/paykit-server issue #26](https://github.com/pubky/paykit-server/issues/26), section “Marketplace lifecycle contract decisions (2026-10-09)”, read at issue update `2026-10-09T09:50:24Z`.
- Required implementation baseline: `origin/master` at `a53268f61b7dfa210c624b0f8b21fd6383e39ab8`.
- PRs #64 and #65 are included in that baseline. The retained-history, stable-ID SDK retry from #65 is the accepted publication handoff guarantee. No separate pre-SDK fence is required.
- This ledger records accepted issue decisions. It does not authorize production merge or deployment.

## Shared constraints

- Use unified trusted-service authentication: canonical signed method, query-free path, and body bytes. Any allowlisted service key has equal route authority. Credential fingerprint is not operation identity.
- Marketplace routes are `POST /marketplace/payment-requests/{prepare,activate,void,resolve}`.
- All request schemas are closed. Existing `{ "error": { "code", "message" } }` envelope and Paykit error conventions apply.
- Keep existing Locks `POST /invoices` lifecycle and `(creator, bundle_id)` identity unchanged.
- Do not add `stack_id`, per-order endpoint persistence, route-specific principals, or caller-owned SDK idempotency keys.

## Preparation contract — current implementation slice

Request:

```json
{
  "creator": "<Pubky>",
  "reader": "<Pubky>",
  "reference": "<payment reference>",
  "amount_sats": 1,
  "operation_id": "marketplace-payment:<payment_reference>:<bind_attempt>",
  "payment_window_seconds": 86400
}
```

Success `200`:

```json
{
  "invoice_id": "<server-issued id>",
  "state": "prepared",
  "total_sats": 1,
  "prepare_expires_at": "<RFC3339 UTC>"
}
```

Accepted behavior:

1. Scope caller-chosen `operation_id` by Marketplace workflow namespace and Creator. Keep it separate from SDK `payment_reference`.
2. Hash the complete normalized immutable accepted request separately from operation identity.
3. Same scoped operation identity plus same binding returns the stored `200` response, including after preparation TTL expiry.
4. Same scoped operation identity plus changed binding returns structured `409` before mutable external work. A new operation ID creates a new invoice.
5. Exact replay/conflict resolution precedes session checks, address allocation, outbox insertion, and SDK work.
6. `payment_window_seconds` is a positive integer. Bind it immutably during preparation and reject values above the relevant configured policy cap; never clamp and never accept an absolute payment deadline.
7. Preparation time and expiry use the database clock. Configured prepare TTL defaults to 15 minutes.
8. Preparation remains unpublished: no claimable proposal outbox row and no SDK publication or cancellation.
9. Do not return a payment deadline during preparation. Payment window begins only when activation commits.
10. Reject unknown fields and fork-only fields including `stack_id`, `allocation_mode`, `nonce_sats`, address fingerprint, and caller-provided `resolved_at`.

## Activation and void contract — current implementation slice

- Activate: closed `{creator, invoice_id, total_sats}` request; DB-clock activation starts the bound payment window; atomically wins against void; inserts publication work once; replay returns stored success.
- Void: closed `{creator, invoice_id}` request; only prepared invoices transition; no publication or SDK cancellation; replay returns stored success.
- Activation at or after `prepare_expires_at`, after void, or after prepared-state resolution conflicts. The authoritative stored total must match. Void remains permitted for a resolved preparation.
- Prepared rows have no outbox work. Activation updates lifecycle state and inserts one encrypted proposal outbox intent in one PostgreSQL transaction. Marketplace outbox ownership remains separate from Locks invoice and drain linkage.
- Activation applies the existing configured proposal-acceptance window, bounded below the selected payment deadline for short caller-bound payment windows. Stable outbox UUID remains the SDK Payment Request ID on every worker retry.

## Resolution contract — current implementation slice

- Resolve: closed `{creator, invoice_id, outcome}` request where outcome is `paid_manually`, `refunded`, or `abandoned`; DB owns `resolved_at`; same outcome replays and a different outcome conflicts; annotation never rewrites protocol or Bitcoin facts.
- Resolution of a prepared invoice blocks later activation but does not block void.

## Verification required for resolution slice

- Signed production route and closed request/response/error fixtures.
- PostgreSQL-backed same-outcome replay and different-outcome conflict races across independent stores.
- DB-authoritative stored timestamp and schema-level outcome immutability.
- Prepared resolution versus activation serialization; no publication when resolution wins first.
- Resolution on prepared, active, and voided rows without Payment Request, Bitcoin observation, or SDK cancellation mutation.

## Verification required for preparation slice

- Signed production route and closed-schema coverage.
- PostgreSQL-backed new preparation, exact replay, changed-binding conflict, and new-operation-ID tests.
- Replay after TTL expiry.
- Positive duration and configured-cap rejection before side effects.
- DB-clock 15-minute default expiry evidence.
- No outbox or SDK publication before activation.
- Existing Locks invoice behavior remains green.

## Verification required for activation and void slice

- Signed production routes and closed request/response/error fixtures.
- Exact activation and void replay with stored DB timestamps.
- Authoritative-total, preparation-expiry, prepared-resolution, and late competing transition conflicts.
- Independent PostgreSQL stores race activate against void on one row; exactly one transition commits.
- No claimable work while prepared or voided; activation admits exactly one proposal intent.
- Existing stable-ID outbox handoff consumes the Marketplace proposal without a second lifecycle state machine.
