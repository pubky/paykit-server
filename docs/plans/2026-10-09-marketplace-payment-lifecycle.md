# Marketplace Payment Lifecycle Decision Ledger

Status: preparation, activation, and void slices authorized; resolution deferred.

## Authority and baseline

- Authoritative product contract: [pubky/paykit-server issue #26](https://github.com/pubky/paykit-server/issues/26), section “Marketplace lifecycle contract decisions (2026-10-09)”, read at issue update `2026-10-09T09:50:24Z`.
- Reviewed preparation baseline: `f9079d50424f31ff0a7ca3df3a12ddc43c398ea8` (tree `eb29afe4f58a83c3bb0d769a2bc51ebbcc602445`; merged to `master` with the same tree as `c351f15e6a3da0bc6c9154ebc0fb8b525aba06da`).
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
4. Same scoped operation identity plus changed binding returns structured `409 operation_conflict` before mutable external work. A new operation ID creates a new invoice.
5. Exact replay/conflict resolution precedes session checks, address allocation, outbox insertion, and SDK work.
6. `payment_window_seconds` is a positive integer. Bind it immutably during preparation and reject values above the relevant configured policy cap; never clamp and never accept an absolute payment deadline.
7. Preparation time and expiry use the database clock. Configured prepare TTL defaults to 15 minutes.
8. Preparation remains unpublished: no claimable proposal outbox row and no SDK publication or cancellation.
9. Do not return a payment deadline during preparation. Payment window begins only when activation commits.
10. Reject unknown fields and fork-only fields including `stack_id`, `allocation_mode`, `nonce_sats`, address fingerprint, and caller-provided `resolved_at`.
11. Bound preparation by the same 15-second request deadline as `POST /invoices`, including the process-local operation-lock wait, session and Reader checks, receiving-detail lookup, and entry into durable storage. Deadline exhaustion returns `503 dependency_timeout`. Once a PostgreSQL mutation begins, await its factual commit or rollback result rather than canceling it into an ambiguous response.
12. Retry only a cleanly missing Reader App Registry, with three total reads and the shared full-jitter delay policy. Exhausted absence remains `reader_setup_pending`; malformed and unavailable reads keep their distinct errors.
13. A seller without Bitcoin receiving details returns `503 seller_setup_pending`, distinct from malformed buyer input and Reader setup.
14. PostgreSQL Creator-row locking and the post-lock operation read serialize commits across processes. The process-local operation mutex only suppresses duplicate external reads within one server process; it is not a horizontal-replica persistence fence.

## Activation and void contract — current implementation slice

- Activate: closed `{creator, invoice_id, total_sats}` request; DB-clock activation starts the bound payment window; atomically wins against void; inserts publication work once; replay returns stored success. Activation derives proposal acceptance duration as `min(configured proposal_acceptance_window, bound payment_window / 2)`. Duration arithmetic retains subsecond precision: a bound one-second payment window yields a 500-millisecond acceptance duration, not zero.
- Void: closed `{creator, invoice_id}` request; only prepared invoices transition; no publication or SDK cancellation; replay returns stored success.
- Activation at or after `prepare_expires_at` returns `409 prepare_expired`, signaling a new attempt. Activation after void or prepared-state resolution, and void after activation, return `409 lifecycle_terminal`, signaling terminal stop. An authoritative stored-total mismatch returns `409 total_mismatch`, signaling operator alert. Exact activation and void replay still return stored `200` responses. Void remains permitted for a resolved preparation.
- Prepared rows have no outbox work. Activation updates lifecycle state and inserts one encrypted proposal outbox intent in one PostgreSQL transaction. Marketplace outbox ownership remains separate from Locks invoice and drain linkage.
- Activation applies `min(configured proposal_acceptance_window, bound payment_window / 2)` exactly, including subsecond results. Stable outbox UUID remains the SDK Payment Request ID on every worker retry.

## Deferred lifecycle contract

- Resolve: closed `{creator, invoice_id, outcome}` request where outcome is `paid_manually`, `refunded`, or `abandoned`; DB owns `resolved_at`; same outcome replays and a different outcome conflicts; annotation never rewrites protocol or Bitcoin facts.
- Resolution of a prepared invoice blocks later activation but does not block void.

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
