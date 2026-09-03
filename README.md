# Paykit Server

A PostgreSQL-backed, receiver-side Paykit prototype for Locks invoice workflows. It derives and observes a direct invoice-specific Bitcoin address. It does not use payer identity, payer inbox messages, or payment-proof messages to attribute payment.

This repository is pre-production. Persisted-data compatibility, stable releases, and production deployment support are not yet provided.

## Development quickstart

The workspace pins Rust in [`rust-toolchain.toml`](rust-toolchain.toml). PostgreSQL is required only for the database-backed E2E suite.

```bash
git clone https://github.com/pubky/paykit-server.git
cd paykit-server
cargo check --locked
cargo test --locked -p paykit-server -- --test-threads=1
cargo test --locked -p paykit-server-e2e --no-run
```

To run the database-backed E2E suite, set `TEST_DATABASE_URL` to a PostgreSQL database whose role may create and drop databases:

```bash
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test --locked -p paykit-server-e2e -- --test-threads=1
```

The two live-adapter tests remain ignored by default because they require either a local Pubky Core static testnet or a recorded public Electrum fixture. See [`docs/live-adapter-smoke.md`](docs/live-adapter-smoke.md) before running them explicitly.

Before submitting changes, read [`CONTRIBUTING.md`](CONTRIBUTING.md). Report security problems through the private process in [`SECURITY.md`](SECURITY.md), not a public issue.

## Executable boundary

`paykit-server` composes and supervises the production HTTP routes, Paykit delivery workers, and BDK Electrum observer in one process.

Public operational routes:

- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Business routes:

- `GET /setup`
- `POST /setup/{flow_id}/complete`
- signed `POST /invoices`
- signed `POST /transactions/status`
- signed `POST /setup/status`

Business-route signatures use the configured trusted Locks Ed25519 key. Setup uses the Bitkit Pubky Auth companion-claim flow and an exact configured browser origin.

`POST /setup/status` is the Locks-only readiness check for an authenticated Creator. Its closed canonical body is `{"creator":"pubky..."}`; the signature covers the exact compact canonical JSON bytes. It returns exactly one coarse state: `ready` when the persisted Creator session imports and matches that Creator, `setup_required` when authority is absent, invalid, expired, or rejected with the Pubky 0.11 status used for revoked grants, and `unavailable` for validation timeouts and transient storage, rate-limit, server, DNS, or transport failures. Callers must not convert `unavailable` into a new authorization flow.

### Setup iframe

`GET /setup` is the production Bitkit setup surface. On desktop it renders the
normal secret-bearing Pubky Auth request as a QR code; on touch devices it
offers the same request through a `Continue with Bitkit` deep link. Production
has no companion handle, helper endpoint or state, helper UI, or helper in the
production package/runtime surface.

The iframe continues polling `POST /setup/{flow_id}/complete`. It never sends
the auth URL, Creator secret, xpub, or companion payload through `postMessage`.
There is no manual claim route. Completion posts only
`{ type: "paykit-setup-callback", state }` or the same callback with a coarse
error to the exact caller origin.

The local Locks demo substitutes a Paykit-owned Cargo example for Bitkit. That
example is built and installed only by `Dockerfile.local`; it is not a normal
package binary or production server surface. It accepts exactly one closed
version-1 JSON object on stdin containing `auth_url`, `creator_secret`,
`account_xpub`, and `account_index`, invokes the canonical `paykit-sdk`
companion-approval operation, and returns only a coarse result. It accepts no
URL, secret, or xpub through argv or `postMessage` and never writes those values
to output. It does not accept a Paykit Server URL or perform a helper-to-server
exchange. See [`docs/local-locks-demo.md`](docs/local-locks-demo.md) for the
local-only logging and trust boundary.

The composed PostgreSQL workflow is tested with two independent Creators across restart. Live adapter evidence covers a separate local Pubky relay/homeserver process and one public mainnet Fulcrum endpoint; see [`docs/live-adapter-smoke.md`](docs/live-adapter-smoke.md). Those checks bound interoperability to the recorded versions and environments rather than claiming compatibility with every provider.

## Deployment model and Creator cardinality

Run exactly **one Paykit Server process** for a deployment. Horizontal replicas and active-active operation are unsupported because setup flows are memory-only and Creator SDK runtimes are process-cached. PostgreSQL locks, constraints, and leases provide concurrency control and crash recovery inside this one-process model, not multi-replica coordination.

One process may own multiple Creator accounts. Each Creator has independent:

- Pubky identity/session and receiver Noise key;
- one BIP84 account xpub and hardened account index;
- external-chain address derivation counter;
- Paykit SDK state and Encrypted Links;
- encrypted invoices, assignments, outbox work, and Bitcoin observations.

Different Creators may use the same numeric child index because their xpubs and derivation sequences are isolated. There is no configured Creator-count limit, but all loaded runtimes remain cached until process exit; practical cardinality is therefore bounded by process and database capacity.

## Persistence, startup, and upgrades

PostgreSQL is the only production persistence backend. SQLite and in-memory adapters are test support only.

Startup holds a session advisory lock while applying the single schema baseline. Before binding HTTP it verifies immutable deployment metadata and authenticates every persisted Creator credential, SDK-state envelope, invoice payment record, and Bitcoin observation. Missing, corrupt, swapped, conflicting, or wrong-key state aborts startup with a secret-free error.

Immutable deployment values are:

- Bitcoin network;
- Paykit Pubky client ID;
- Paykit receiver path;
- trusted Locks public-key fingerprint.

Changing any of them after database initialization requires resetting the database.

Persisted application and schema compatibility across releases is intentionally unsupported during this pre-production phase. When an upgrade changes the baseline migration or a persisted payload representation:

1. stop the old process;
2. discard and recreate the Paykit Server database;
3. start the new binary so it applies the current baseline.

The cryptographic envelope version, domain-separated KDF/AAD labels, and private payload format discriminators remain enforced. They detect unsupported or corrupt bytes; they are not compatibility readers.

## Configuration and secrets

Copy [`config/paykit-server.example.toml`](config/paykit-server.example.toml) to an operator-controlled path. The TOML schema is closed: unknown sections and keys are rejected. Durations are strings such as `"10s"` and `"5m"`.

Required environment variables:

- `PAYKIT_CONFIG` — path to the TOML file;
- `PAYKIT_DATABASE_URL` — PostgreSQL connection URL;
- `PAYKIT_MASTER_KEY` — unpadded base64url encoding of exactly 32 bytes.

Do not put database credentials or the master key in TOML, logs, shell history, or source control. Effective-config debug output redacts secret values.

`setup.log_authorization_url` defaults to `false` and must remain false for
production. The paired Locks correction will make its generated local-demo
config the sole `true` setting; once that sibling change lands, each new setup
flow emits one explicitly labeled authorization URL log line for operator
retrieval. The URL is a bearer secret; the local operator owns access to and
retention of those logs.

The parser rejects the retired `[inbox]` section. The executable exposes no payer
inbox API or worker, and the baseline schema contains no payer inbox tables.

`paykit.network = "testnet"` selects the pinned Pubky client’s fixed **local** testnet configuration. It requires the Pubky Core static testnet on localhost; it is not a hosted public testnet. `paykit.network = "mainnet"` uses normal Pkarr/homeserver resolution. Bitcoin network and Electrum endpoint are configured separately and must agree.

The executable consumes only keys shown in the example. Arbitrary Paykit relay/homeserver URLs are not accepted.

`paykit.client_id` is required and must be exactly `"app.paykit.server"`. It is an
immutable deployment invariant, not an optional label. Missing configuration now
fails with the direct error `paykit.client_id is required`.

## Running

With configuration and secrets supplied by an operator-controlled secret manager:

```bash
cargo run -p paykit-server -- --check-config
cargo run -p paykit-server
```

`--check-config` validates the complete TOML and required environment values,
prints only `configuration valid`, then exits before PostgreSQL connection,
migration, network construction, or HTTP bind. Run it against the exact staged
config and environment before restarting a deployment.

For systemd deployments, gate startup and bound invalid-config restart storms:

```ini
[Unit]
StartLimitIntervalSec=60
StartLimitBurst=3

[Service]
ExecStartPre=/usr/local/bin/paykit-server --check-config
ExecStart=/usr/local/bin/paykit-server
Restart=on-failure
RestartSec=5s
```

Both commands must receive the same `PAYKIT_CONFIG`, `PAYKIT_DATABASE_URL`, and
`PAYKIT_MASTER_KEY` environment. Adjust executable path to deployment layout.

Startup fails before bind if configuration, secrets, PostgreSQL, migrations, authenticated persisted state, or immutable deployment values are invalid. Electrum need not be reachable at construction time; its worker reports degraded health and retries.

### Local Locks demo image

`Dockerfile.local` packages this repository for the Locks Compose stack. It
accepts pinned public Git sources or deliberate local-worktree overrides through
named BuildKit contexts, then produces an unprivileged local image containing
the server, the existing reader-demo binary, and the local companion Cargo
example installed as `paykit-companion-auth`.

Build command, image contract, source-rewrite behavior, and generated config contract live in [`docs/local-locks-demo.md`](docs/local-locks-demo.md).

## Health, readiness, metrics, and shutdown

- `GET /health/live` returns `200` with `{ "status": "live" }` while the process serves; it performs no dependency check.
- `GET /health/ready` reports `postgres`, `electrum`, `paykit_delivery`, and `outbox` states. Overall `ready` and `degraded` return `200`; `not_ready` returns `503`.
- PostgreSQL loss is `not_ready`. Electrum or Paykit delivery trouble is `degraded`.
- `GET /metrics` exports identifier-free Prometheus/OpenMetrics data.

Health and metrics do not expose Creator/reader identities, addresses, URLs,
payloads, signatures, credentials, or protocol correlations. Normal production
logs have the same boundary. The sole exception is the explicitly enabled local
demo authorization-URL event described above. Policy rate limiting returns
`429`; exhausted runtime admission returns `503` with `Retry-After: 1`.

Setup completion emits secret-free structured events with
`event="paykit_setup_completion"` and closed `stage`, `outcome`, and `class`
fields. Stages cover AUTH completion, identity/session handling, companion relay
receive, claim verification, xpub validation, setup locking, marker
publish/readback, persistence/compensation, lock release, and relay ACK. These
events intentionally omit flow IDs, Creator identities, authorization and relay
URLs, sessions, xpubs, payloads, and raw error text; correlate them by timestamp
and request access logs.

On SIGTERM or SIGINT, readiness changes first, normal admission and new worker claims stop, and admitted work drains for at most `shutdown.drain_timeout`. Remaining work is cancelled at the deadline. Durable leases can be reclaimed after restart; pending memory-only setup flows are lost.

## Invoice and delivery semantics

A successful new invoice transaction atomically persists the Creator/reader assignment, invoice, complete endpoint-publication intent, complete dependent Payment Request intent, and address allocation. Exact replay preserves that durable result; conflicting replay is rejected.

The later SDK handoff is not exactly once. Server delivery is at least once:

- a crash before SDK-generated identifiers are durably associated may enqueue another Payment Request with new SDK Event, Payment Request, and outbound-message identifiers;
- consumers must tolerate duplicate proposals and use stable server intent/Payment Reference values where applicable;
- Private Payment List is latest-state data, so identical re-enqueue supersedes safely;
- marking server work delivered means the exact SDK outbound record reached SDK `Sent`, not that the remote application acknowledged it.

The invoice API returns after durable intent commit. It does not wait for Encrypted Link establishment or remote delivery.

## Bitcoin settlement semantics

Each invoice receives a unique BIP84 external-chain address. Observation uses the configured `bdk_electrum` adapter and persists complete validated batches atomically.

- Outputs are evaluated independently; split or multi-output payments are not aggregated.
- A single amount-matched output is sufficient for the factual amount match.
- An underpaying output remains a replaceable factual underpayment at every confirmation depth.
- A one-confirmation amount-matched output is frozen against replacement while monitoring continues.
- At six confirmations, an amount-matched output becomes final with stored/reported confirmation count exactly `6`, and monitoring for that invoice stops.
- Overpayment is factual but has no credit/refund workflow.
- Reorg handling is supported before finality; uncommon repair after six-confirmation finality is unsupported.

The server has no Bitcoin spending keys and cannot spend, refund, or create change.

## Payer, proof, and receipt exclusions

The server does not process payer-originated acceptance, rejection, cancellation, inbox, or payment-proof events. It exposes no payer inbox and no proof-submission API. Direct invoice-address observation is the only payment-attribution input.

Paykit Receipt issuance, Receipt Access delivery, and receipt storage are unsupported.

## Retention and data lifecycle

There is no payload-retention or pruning contract, retention worker, runtime idle eviction, or SDK compaction contract. Operators must treat encrypted Creator, SDK, invoice, assignment, outbox, internal relay, and Bitcoin observation records as retained according to current database/SDK behavior. Any deletion policy requires a separate product and migration decision.

## Known limitations

- One process only; no replicas or active-active deployment.
- One Electrum endpoint; no failover pool.
- `tcp://` Electrum has no transport authentication; use a CA-valid `ssl://` endpoint for production.
- No configured Creator-count bound or runtime eviction.
- One xpub/account index per Creator; no xpub, account, master-key, or immutable-invariant rotation.
- BTC only; no other assets.
- No payer inbox/proofs or receipt workflows.
- No spending custody, refunds, credits, or change.
- No output aggregation and no deep-reorg repair after finality.
- No retention/pruning contract.
- Live Pubky evidence uses a local static testnet, not a remote production homeserver or the complete Bitkit user-approval journey.
- Live Electrum evidence proves one exact mainnet Fulcrum snapshot over plaintext protocol; it is not a production TLS endorsement.
