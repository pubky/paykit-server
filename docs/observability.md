# Paykit Server observability

Paykit Server emits structured JSON through existing `tracing` setup. Production enables `paykit_server=info` only; dependency targets stay disabled because upstream diagnostics can contain URLs, identities, or response text.

Logs show **last observed application progress**, not authoritative audit history. PostgreSQL remains source of persisted truth. Process crashes, host loss, delivery failure, sampling, rotation, or backend outage can leave no terminal log. After restart, workers correlate with durable entity/job IDs; they do not invent or recover an earlier ephemeral HTTP request ID.

## IDs and privacy

| Field | Scope / lifetime | Meaning |
| --- | --- | --- |
| `request_id` | One HTTP exchange; client value accepted only as canonical UUIDv4 | Transport correlation echoed in `X-Request-ID`. Not authorization and not guaranteed unique: clients may repeat it. |
| `request_instance_id` | One in-process HTTP middleware invocation | Server-generated UUIDv4 that distinguishes requests even when a client repeats `request_id`. Not persisted. |
| `setup_trace_id` | One in-memory setup flow | Server-generated UUIDv4 safe replacement for secret-bearing setup flow token. Links begin, completion task, and state changes until process exit. |
| `invoice_id` | Durable invoice | Server-owned UUID emitted only after validation/persistence or from trusted persisted state. |
| `outbox_job_id` | Durable outbox row | Server-owned UUID linking committed invoice work to worker processing. |
| `creator_id` | Durable internal server account | Internal UUID. Worker-only correlation; never a metrics label. |
| `payment_request_id` / `payment_reference` | Durable Paykit domain object when available | Emit only canonical typed values at validated/persisted boundaries. Do not substitute bundle, creator, Noise, or request IDs. |
| `entity_kind` / `entity_id` | Durable observer target | Closed kind (`invoice` or `marketplace_preparation`) plus persisted UUID used when one event schema spans both stores. |

Opaque IDs remain correlation-sensitive. Restrict log access, encrypt transport/storage, apply operator retention limits, and avoid sharing raw queries outside incident scope. Logs never contain request bodies, raw query strings, setup flow IDs, cookies, auth/session/grant material, authorization URLs (except explicit local-only `setup.log_authorization_url`), QR contents, private keys, xpubs, preimages, claims, proofs, payment evidence, or raw error/source chains.

## HTTP chain

Every operational route emits at INFO:

1. `paykit_http_request_started`
2. zero or more bounded stage/domain events
3. `paykit_http_request_completed`

Common fields: `operation`, normalized `method`/`route`, `request_id`, `request_instance_id`. Terminal events add `http_status`, `failure_class`, and `elapsed_ms`. Cancellation or panic before a response has no fabricated status and records closed `cancelled` or `panic` outcome. Existing narrow `paykit_request_failure` warnings remain limited to one per request and carry only closed `stage`/`category` values.

Covered routes:

- setup: `GET /setup`, `GET /setup/reconnect`, `GET /setup/creator-held`, `POST /setup/{flow_id}/complete`, `POST /setup/status`;
- invoices/status: `POST /invoices`, `POST /v2/invoices`, `POST /v2/invoices/status`, `POST /transactions/status`, `POST /connections/status`, `POST /payment-requests/status`;
- drains: create, lookup, cleanup routes;
- marketplace: prepare, activate, void, resolve, status, receipt routes.

`GET /health/live`, `GET /health/ready`, and `GET /metrics` are intentionally excluded: probes are high-volume, contain no business workflow, and already have bounded responses/metrics. Unknown/static paths are excluded. Route fields are fixed templates; raw path parameters and query strings are never logged.

## Setup lifecycle

`paykit_setup_request_link` bridges each HTTP invocation to a setup flow using `request_id`, server-owned `request_instance_id`, and `setup_trace_id`. Begin and every later completion poll emit this bridge after resolving the in-memory flow; repeated client request IDs therefore remain separable while polls still join the same workflow. Unknown/expired flow tokens have no retained trace ID and cannot be linked. The bearer `flow_id` is never logged.

`paykit_setup_flow_transition` records `previous_state`, `new_state`, `outcome`, `error_class`, and `setup_trace_id`. Normal path:

```text
none -> pending -> completing -> completed
```

One-shot failure ends `completing -> failed`; a successful event is emitted only after completer reports durable success. Spawned completion explicitly keeps logging subscriber context while correlation uses `setup_trace_id`, never bearer `flow_id`. Poll HTTP requests each receive their own HTTP IDs and link to flow only through setup lifecycle trace.

## Domain and worker milestones

`paykit_invoice_persisted` is emitted only after atomic persistence returns, with `truth=persisted`, `invoice_id`, and `outbox_job_id`. `outcome=created|replayed` distinguishes new commit from idempotent replay.

`paykit_outbox_attempt_started` marks each durable lease with `creator_id`, `outbox_job_id`, canonical `payment_request_id`, proposal `payment_reference` when applicable, `attempt`, and `state=leased`. `paykit_outbox_transition` is emitted only after a fenced durable state update succeeds. Enqueue transitions retain those IDs and add `previous_state`, `new_state`, and `outcome`. Retry events add closed `retry_class` plus `backoff_ms`; handoff events add SDK `outbound_message_id`. Reconciliation events reuse persisted `creator_id`, `outbox_job_id`, and `outbound_message_id` across restarts, recording `handed_off -> delivered` only after durable SDK acknowledgement, or retained retry/permanent-failure state after its database update. `paykit_outbox_transport_deferred` reports only closed `category` values and durable IDs; raw SDK/provider errors are not formatted.

`paykit_payment_request_lifecycle_transition` is emitted after an attributable lifecycle transaction commits. It carries durable `creator_id`, canonical SDK `payment_request_id`, `new_state`, `truth=persisted`, and `outcome=applied|replayed`. Changed state (`applied`) is INFO; unchanged exact replay is DEBUG. Unattributable SDK records emit no domain-ID event. `paykit_payment_request_lifecycle_pass` carries `creator_id` plus closed pass failure/defer categories; routine unchanged receive details remain DEBUG.

Other composed worker boundaries retain only IDs already authenticated by persistence. `paykit_shared_transport_pass` carries `creator_id` for targeted success and every failure; routine full-scan success stays quiet and shared-state deferral stays DEBUG. Allowance intake, seller-receipt, and per-Creator panic events carry their owning `creator_id` but do not invent invoice/job IDs when no single durable entity exists. `paykit_buyer_contact_transition` carries persisted `creator_id` plus `invoice_id`, and reports completion only after contact storage succeeds. Electrum failures emit `paykit_electrum_observation_failure` per authenticated invoice or Marketplace target with `creator_id`, `entity_kind`, and `entity_id`; raw addresses, outpoints, endpoints, and provider errors remain absent. Target-enumeration failures cannot be attributed safely and remain aggregate events.

Corrupt outbox intent cannot supply trustworthy payment IDs. Its lease-start and successful fenced terminal transition therefore carry only persisted `creator_id`, `outbox_job_id`, optional `invoice_id`, and `attempt`, with `correlation=unavailable` / closed `corrupt_or_missing` category. A stale fenced update emits no successful transition; production reports the existing `paykit_outbox_transition_skipped` terminal outcome instead.

Expected retry timeline:

```text
paykit_invoice_persisted(outbox_job_id, invoice_id)
paykit_outbox_transition(leased -> retryable, retry_class, backoff_ms)
paykit_outbox_transition(leased -> handed_off, payment_request_id, outbound_message_id)
paykit_outbox_transition(handed_off -> delivered, outbound_message_id)
paykit_payment_request_lifecycle_transition(new_state, payment_request_id)
```

A restart resumes with durable creator/job/entity IDs and a later `attempt`; it does not recover the original ephemeral HTTP IDs. INFO contains persisted changes and terminal/deferred milestones. Repeated unchanged polls/details stay at DEBUG. Domain IDs never become metrics labels.

## Queries

Examples assume a JSON log backend with field filtering:

```text
request_id="..." OR request_instance_id="..."
setup_trace_id="..."
invoice_id="..." OR outbox_job_id="..."
payment_request_id="..." OR payment_reference="..."
creator_id="..." AND event LIKE "paykit_%"
```

Trace HTTP to setup task: find `paykit_http_request_started` by `request_id`, note normalized route and `request_instance_id`, then follow setup lifecycle by `setup_trace_id`. Trace invoice to worker: find `paykit_invoice_persisted`, then follow `outbox_job_id`. A later worker after restart may have durable job/entity IDs but no initiating `request_id`; this is expected.

## Volume and failures

Default INFO includes HTTP start/outcome and meaningful lifecycle milestones. Repeated unchanged polling/details belong at DEBUG. WARN means closed failure/cancel/panic category, never raw provider text. `SharedStateBusy`, transport, timeout, protocol/auth, and storage categories must remain distinct when typed source errors establish them; never infer revocation or terminal auth state from HTTP status, strings, or debug output.

Startup currently has no verified build-SHA metadata, so logs must not fabricate it. Deployment tooling should identify image/tag externally until service owns trusted build metadata.
