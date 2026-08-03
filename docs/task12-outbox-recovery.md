# Durable semantic outbox recovery

`POST /invoices` atomically persists the invoice, its reader allocation, the endpoint-publication intent, the dependent Payment Request intent, and both encrypted semantic envelopes. Exact replay preserves those durable identities and payloads.

At request time the server discovers capable reader markers and deterministically selects by `paykit.receiver_path_priority` (default `bitkit`), then first path segment and canonical lexical full path. It persists the selected reader path and fingerprint inside the encrypted, Creator- and row-bound delivery intent. Exact replay may bypass discovery because it returns the already authenticated intent.

Workers claim fenced rows, decrypt and revalidate the complete intent, refetch the exact selected marker, and retry if its fingerprint changed. They never reselect another path. Production handoff uses only public Paykit SDK APIs with one encrypted PostgreSQL SDK state per Creator.

A successful public enqueue/proposal stores the returned SDK outbound ID under the same live fence as `handed_off`; Payment Requests also store the returned Event and Payment Request IDs. If the SDK transaction commits before this server transition, reclaimed work calls the public API again. That accepted crash window is at-least-once and may create duplicate Payment Request proposals.

`handed_off` means durable local SDK queue association, not remote delivery. A separately fenced reconciliation claim runs the SDK outbound processor and checks the exact stored outbound ID in durable Creator SDK state. Only `OutboundPrivateMessageStatus::Sent` advances the row to `delivered`, which means successful Encrypted-Link send—not payer application read, processing, or acknowledgement. Endpoint dependents remain blocked until this transition. SDK `Pending`, `Sending`, and retry-backoff `Failed` records remain retryable. `RecoveryRequired` also remains retained and retryable while the separate SDK Encrypted-Link recovery flow is unresolved. `Invalid` and `Superseded` exact records cannot become the required exact `Sent` record and are retained as `permanently_failed`. Missing or changed SDK state is retried; permanent reconciliation errors retain only a non-secret error class.

The baseline schema requires new `handed_off` and `delivered` rows to carry a canonical numeric SDK outbound ID. Earlier prototype rows are not migrated; operators must reset the database when adopting this baseline.

No part of this design claims exactly-once remote delivery.
