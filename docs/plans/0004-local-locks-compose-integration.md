# Local Locks Compose Integration Implementation Plan

> **For Hermes:** Use `subagent-driven-development` to implement this plan task-by-task only after every implementation gate below is resolved. Stop after each commit-sized task for manual review.

**Goal:** Package Paykit Server for the sibling Locks Compose demo, make its setup iframe suitable for local CLI approval, and provide Paykit-native helper binaries that substitute for Bitkit without bypassing companion-auth or Payment Request protocols.

**Architecture:** Paykit Server retains its existing setup, companion relay, creator persistence, invoice, delivery, and Bitcoin observation boundaries. Local helper binaries call public `paykit-sdk` APIs and run in Locks demo containers; they do not add server bypass routes. `Dockerfile.local` is explicitly local/demo packaging for Pubky testnet, Bitcoin regtest, local PostgreSQL, and local Electrum—not a claimed production image.

**Tech stack:** Rust 1.91.1, Axum, `paykit-sdk`, PostgreSQL/SQLx, Docker multi-stage builds, Pubky static testnet, Bitcoin Core regtest, BDK Electrum.

**Publication note:** The original peer implementation plan was intentionally
not included in the fresh public Locks history. The public cross-service
contract is [Locks ADR 0020](https://github.com/pubky/locks/blob/b3a054fcdfc5e4bc21b6daa2f9e48138bc6375c4/docs/ADRs/0020-locks-paykit-v1-integration-boundary.md).
Public image builds use anonymous pinned Git contexts; sibling worktrees remain
an explicit local-development override.

---

## Requirement provenance

### EXPLICIT

- The sibling Locks Compose environment must include Paykit Server.
- Selecting `paykit-payment` embeds a Paykit Server-hosted setup iframe.
- The iframe shows Paykit auth URL and exact local CLI instructions.
- The iframe does not receive or know xpub/tpub.
- No separate manual xpub HTTP route is allowed.
- A local script accepts auth URL and xpub/tpub and uses the same companion-auth flow expected from Bitkit.
- Compose must support full PostgreSQL + Bitcoin regtest + Electrum payment testing.
- A protocol-real reader helper replaces Bitkit for marker publication and Payment Request receipt.
- Detailed plans must be written in both repositories before implementation.

### AUTHORITATIVE SOURCE

- [Paykit companion-claim specification](https://github.com/pubky/paykit-rs/blob/v0.1.0-rc48/specs/pubky-auth-companion-claims.md).
- [Paykit SDK companion-claim implementation](https://github.com/pubky/paykit-rs/blob/v0.1.0-rc48/paykit-sdk/src/pubky_session/companion_claim.rs).
- `paykit-server/src/bitkit_claim.rs` for exact `watch-only-account-v1` payload and receiver validation.
- `paykit-server/src/real_setup.rs` for setup commit ordering and xpub validation.
- `README.md` and `config/paykit-server.example.toml` for supported runtime/config behavior.
- Public peer contract: [Locks ADR 0020](https://github.com/pubky/locks/blob/b3a054fcdfc5e4bc21b6daa2f9e48138bc6375c4/docs/ADRs/0020-locks-paykit-v1-integration-boundary.md).

### CONSTRAINTS OBSERVED IN CURRENT CODE

- `GET /setup` currently starts setup and immediately navigates iframe to the secret-bearing auth URL.
- `POST /setup/{flow_id}/complete` already waits for normal Pubky AUTH plus companion relay claim, then validates/publishes/read-backs/persists.
- Exact local-demo companion request is `x-bitkit-claim=watch-only-account-v1` with capabilities `/pub/paykit/v0/bitkit/server/:rw,/pub/paykit/v0/private/bitkit/server/:rw`; production derives the equivalent public/private pair from the configured server receiver path.
- Unsigned payload is 84 bytes: version byte, big-endian account index, reserved byte, 78-byte serialized BIP32 account xpub.
- `paykit-sdk::PubkySessionBootstrap::approve_auth_with_companion_claim` validates request, signs, encrypts, relays claim, and only then approves Pubky AUTH.
- Paykit Server has no Dockerfile.
- Startup requires `PAYKIT_CONFIG`, `PAYKIT_DATABASE_URL`, and 32-byte base64url-no-pad `PAYKIT_MASTER_KEY`.
- Startup runs PostgreSQL initialization before bind; Electrum failure is degraded rather than bind-fatal.
- Current workspace dependencies pin Git revisions for Locks and Paykit SDK; local Docker build context must preserve reproducibility and SSH-free build access.

## Accepted decisions

1. Keep the existing standard server companion receiver; do not add a manual claim route or bypass mode.
2. Setup iframe displays auth URL + approved Docker CLI command instead of automatically navigating.
3. iframe never receives, stores, posts, or logs xpub.
4. Companion CLI is Paykit-owned Rust, invokes public `paykit-sdk`, and runs inside Locks `creator-demo`.
5. Locks Node wrapper loads creator key and pipes secret/auth URL/xpub/account index to helper stdin; sensitive values never enter argv/logs.
6. Reader helper uses Paykit SDK to publish reader marker, persist reader SDK state, and receive/decrypt Payment Requests.
7. Reader helper prints address/sats, a manual Bitcoin payment command, and a separately labeled optional mining command; it does not pay or mine.
8. Compose includes PostgreSQL, Bitcoin Core regtest, and Electrum indexer.
9. Existing six-confirmation Paykit finality remains, but local Locks uses `minimum_confirmations = 0`; mining is optional and does not gate access.
10. Reader tooling uses two one-shot commands and no daemon:
    - `prepare-paykit-reader` creates or reopens `.local/paykit-reader/`, publishes and reads back the Receiver Marker, persists state, and exits;
    - `receive-paykit-request` reopens that state, waits with a bounded timeout for one Payment Request, prints payment instructions, persists updated state, and exits;
    - `.local/paykit-reader/state.v1` is a versioned XChaCha20-Poly1305 envelope containing Paykit `StorageState` and the Receiver Noise secret;
    - its key uses HKDF-SHA256 over the existing reader Pubky secret with random salt and exact domain `paykit-reader-state-v1`; every atomic rewrite uses a fresh nonce, and the file uses mode `0600`;
    - corrupt or wrong-key state fails closed and is never silently reset.
11. Container packaging is explicitly local/demo-only:
    - this repository owns `Dockerfile.local`;
    - Locks Compose supplies BuildKit `additional_contexts` for local Paykit Server, Paykit Rust, and Locks source trees;
    - Docker-only Cargo patch configuration redirects the pinned Git dependencies to copied local trees without changing committed `Cargo.toml` dependency declarations;
    - builds require no SSH agent/token, compile the exact local branches, and fail clearly when sibling checkouts are missing.
12. Infrastructure images and readiness are pinned:
    - PostgreSQL is `postgres:17-bookworm@sha256:4f736ae292687621d4dbe0d499ffd024a36bd2ee7d8ca6f2ccd4c800f047b394` and readiness uses `pg_isready`;
    - Bitcoin Core is `bitcoin/bitcoin:29.1@sha256:de62c536feb629bed65395f63afd02e3a7a777a3ec82fbed773d50336a739319` and readiness requires authenticated RPC plus regtest-chain validation;
    - Electrum is `cculianu/fulcrum:v1.11.1@sha256:70f06b93ab5863997992d4b4508312fe81ce576017e16ecc7e69c7d38165bdf2`, matching recorded BDK adapter evidence, and readiness performs an Electrum `server.version` request rather than a TCP-only probe;
    - Paykit readiness checks `/health/live` and then `/health/ready`, rejecting `not_ready` while allowing the documented Electrum-degraded startup transition until Fulcrum is ready.
13. Integration runtime state is disposable and managed explicitly because the Pubky static testnet is ephemeral:
    - Locks PostgreSQL, Paykit PostgreSQL, Bitcoin Core, and Fulcrum each use one explicitly named disposable volume; no anonymous infrastructure volume is accepted;
    - ordinary `docker compose down` preserves those four volumes, so service-only or whole-stack restarts can retain runtime state;
    - Locks owns `npm run reset-paykit-demo`, which tears down Compose, removes exactly those four disposable volumes, `.local/bitcoin-bootstrap/` scratch state, and `.local/paykit-reader/`, and preserves generated credentials/config, Lock Server identity, and the three demo role identities;
    - a new testnet generation requires reset/re-registration, and detectable stale state fails clearly rather than being reused.
14. Bitcoin regtest bootstrap:
    - Bitcoin Core runs regtest with server and transaction index enabled, pruning disabled, and RPC exposed only inside the Compose network;
    - Locks setup generates random RPC credentials in untracked `.local/bitcoin-rpc.env` mode `0600`; Bitcoin, Fulcrum, and bootstrap consume them without logging;
    - one-shot `bitcoin-bootstrap` waits for authenticated regtest RPC, creates or loads wallet `miner`, mines 101 blocks to a wallet address, and exits;
    - Fulcrum starts after bootstrap and indexes the regtest chain;
    - tester payment uses `docker compose exec -T bitcoin sh -ec 'bitcoin-cli -conf="$BITCOIN_DATA/bitcoin.conf" -regtest -rpcwallet=miner ...'`; the reader helper prints exact `sendtoaddress` with a canonical eight-decimal BTC amount plus a separately labeled optional `generatetoaddress 6` command.
15. Helper process contract:
    - this repository owns binaries `paykit-companion-auth` and `paykit-reader-demo`; Locks owns user-facing Node wrappers;
    - each invocation accepts exactly one closed, version-1 JSON object on stdin, rejects unknown fields/versions, and never echoes sensitive input;
    - companion input contains `auth_url`, base64url 32-byte `creator_secret`, `account_xpub`, and `account_index`; success output is only version plus `approved` status;
    - reader input contains operation `prepare` or `receive` plus base64url 32-byte `reader_secret`; state path and local Pubky endpoints come only from Compose environment;
    - receive timeout is five minutes;
    - prepare output is limited to reader Pubky/receiver path, receive output to BTC address/sats/manual commands, and failures use stable redacted error codes.
16. Local Locks uses `paykit.minimum_confirmations = 0`:
    - a matching Paykit `detected` mempool observation may satisfy access without mining;
    - Paykit's independent six-confirmation finality behavior is unchanged;
    - optional six-block mining may validate finality but is not part of the access gate.
17. Local topology and config:
    - browser URLs are Locks `http://localhost:3000`, creator `http://localhost:8080`, reader `http://localhost:8088/reader/`, and Paykit `http://localhost:3001`; existing Pubky ports remain unchanged;
    - Paykit listens on `0.0.0.0:3001` in Pubky testnet's network namespace; Locks calls `http://127.0.0.1:3001`;
    - setup allows exact origin `http://localhost:8080` only; parent validates Paykit origin `http://localhost:3001` and exact iframe window;
    - setup callback contains only `{ type: "paykit-setup-callback", state }` or a coarse error; auth URL, flow ID, and xpub never enter `postMessage`;
    - Paykit uses Pubky `testnet`, Bitcoin `regtest`, Electrum `tcp://fulcrum:50001`, receiver path `bitkit/server` with matching derived companion capabilities, priority `["bitkit"]`, Electrum polling `1s`, and outbox polling `500ms`;
    - Locks and Paykit use separate PostgreSQL services backed by explicitly named disposable volumes that ordinary `docker compose down` preserves and `npm run reset-paykit-demo` removes;
    - startup order is both PostgreSQL services plus Bitcoin, 101-block bootstrap, real Fulcrum readiness, Locks identity readiness, one-shot closed Paykit config generation from the actual Locks public key, Paykit readiness, then creator/reader demos.
18. Implementation proceeds Paykit-first:
    - this repository implements iframe, companion helper, reader helper, local Dockerfile/config, with verification and manual-review stop after each commit-sized task;
    - Locks then implements auth parity, payment UI, Paykit iframe coordination, Node wrappers, reader workflow, and Compose, again stopping after each task;
    - cross-repository E2E runs last; fixes and commits stay repository-local, followed by final verification/manual review in each repository.
19. Final acceptance matrix:
    - automated coverage preserves existing APIs, checks setup iframe origin/window/state behavior, helper schema/redaction/encrypted state/restart/corruption, local image/config/readiness, repository tests, Clippy, and formatting;
    - clean-stack E2E completes standard companion setup with local tpub/account index, restarts Paykit, accepts one signed Locks invoice, delivers and decrypts one Payment Request, proves prepayment pending, observes the exact regtest mempool output, reports matching zero-confirmation `detected`, and allows Locks access;
    - optional six-block mining verifies Paykit's independent finality;
    - retry/failure evidence covers invalid xpub/account index with redacted retry, reusable reader state after five-minute timeout, Fulcrum outage/recovery, Paykit restart durability, duplicate exact proof replay without duplicate invoice, wrong address/underpayment denial, and full reset behavior owned by Locks.

## Unresolved implementation gates

None. Implementation still requires explicit final plan approval.

No implementation task may invent an answer to these gates.

---

### Task 1: Plan-only cross-repository checkpoint

**Objective:** Keep this plan and the Locks plan self-contained and contradiction-free.

**Files:**
- Create/Modify: `docs/plans/0004-local-locks-compose-integration.md`
- Review: public [Locks ADR 0020](https://github.com/pubky/locks/blob/b3a054fcdfc5e4bc21b6daa2f9e48138bc6375c4/docs/ADRs/0020-locks-paykit-v1-integration-boundary.md)

**Steps:**
1. Resolve all gates one question at a time.
2. Remove stale suggestions of iframe xpub forms, manual setup routes, or direct database payment discovery.
3. Confirm exact `paykit-rs` public call chain and quote changed dependency APIs by path.
4. Run `git diff --check` in both repositories.
5. Stop for manual review.

**Suggested commit:** `docs: plan local Locks integration`

### Task 2: Render setup instructions without auto-navigation

**Objective:** Preserve existing setup security/callback semantics while making local CLI approval possible.

**Files:**
- Modify: `paykit-server/src/http/setup.rs`
- Test: `paykit-server/tests/setup.rs`
- Modify: `README.md`

**TDD sequence:**
1. Add route tests asserting returned HTML displays the escaped auth URL and approved static CLI command.
2. Assert HTML has no xpub input, claim endpoint, automatic `window.location.assign`, or leaked completion result.
3. Assert CSP/frame-ancestor and concrete postMessage origin behavior remain exact.
4. Render instructions and auth URL while preserving existing completion polling.
5. Run `cargo test -p paykit-server --test setup`.
6. Stop for review.

**Suggested commit:** `feat: show Paykit setup CLI instructions`

### Task 3: Add companion-auth helper binary

**Objective:** Produce the existing Bitkit claim through `paykit-sdk` without protocol duplication in Locks JavaScript.

**Files:**
- Create: `paykit-server/src/bin/paykit-companion-auth.rs`
- Modify: `paykit-server/src/bitkit_claim.rs` only to expose one canonical unsigned-payload encoder if needed
- Add tests in: `paykit-server/tests/setup.rs` or new `paykit-server/tests/companion_auth_cli.rs`
- Modify: `paykit-server/Cargo.toml` only if an explicit binary/dependency entry is required

**TDD sequence:**
1. Test strict stdin schema, unknown-field rejection, 32-byte creator secret, canonical xpub parse, regtest-compatible tpub network validation, bounded account index, and no sensitive debug output.
2. Test exact 84-byte unsigned payload against existing parser fixture.
3. Test helper invokes `PubkyAuthCompanionClaim::new` with canonical query/type and `approve_auth_with_companion_claim` with exact capability.
4. Add relay-backed integration evidence proving companion delivery precedes AuthToken approval.
5. Ensure errors are coarse/redacted and stdout emits only stable success metadata.
6. Run focused tests and Clippy.
7. Stop for review.

**Suggested commit:** `feat: add companion auth test helper`

### Task 4: Add reader Paykit helper binary

**Objective:** Replace reader-side Bitkit with a Paykit-native stateful helper, not a server/database shortcut.

**Files:**
- Create: `paykit-server/src/bin/paykit-reader-demo.rs`
- Add tests in: `paykit-server/tests/reader_demo_cli.rs`
- Reuse: public `paykit-sdk` marker, storage, encrypted-link, and Payment Request APIs

**TDD sequence:**
1. Test strict stdin/config schema and redaction.
2. Test deterministic import of reader identity and persistent receiver Noise/SDK state in versioned XChaCha20-Poly1305 `.local/paykit-reader/state.v1`, including HKDF-SHA256 with exact domain `paykit-reader-state-v1`, random salt, fresh nonce, atomic `0600` rewrite, and wrong-key/corrupt-state fail-closed behavior.
3. Implement `prepare-paykit-reader` as a one-shot operation that publishes/read-backs the canonical Receiver Marker, persists state, and exits.
4. Implement `receive-paykit-request` as a one-shot bounded wait that reopens state, receives/decrypts one Payment Request, emits only address, asset, amount, and stable request identity needed by the tester, persists updated state, and exits.
5. Reject non-BTC, malformed, wrong-recipient, duplicate-conflicting, or unsupported request shapes according to dependency contracts.
6. Print the payment command and a separately labeled optional mining command without invoking Bitcoin RPC.
7. Prove restart can reopen local reader state without rotating required keys.
8. Stop for review.

**Suggested commit:** `feat: add Paykit reader demo helper`

### Task 5: Add reproducible local container packaging

**Objective:** Build Paykit Server and both helper binaries for the Locks Compose environment.

**Files:**
- Create: `Dockerfile.local`
- Create: `.dockerignore`
- Possibly create: `docker/entrypoint.sh` only if direct binary command/config is insufficient
- Modify: `README.md`

**TDD/verification sequence:**
1. Pin Rust toolchain/runtime base images and accept named BuildKit contexts for local Paykit Rust and Locks source trees; the sibling Locks Compose plan owns the accepted PostgreSQL, Bitcoin Core, and Fulcrum image pins.
2. Generate Docker-only Cargo source patches to local copied trees; ensure committed dependency declarations remain unchanged and builds need no host SSH credentials.
3. Build server and helper binaries from the exact local branches in a Rust builder stage.
4. Copy only runtime artifacts/config necessities into non-root runtime image where supported.
5. Add image-level smoke for `paykit-server` and helper `--help`/stdin rejection.
6. Verify no source secrets, Cargo credentials, or target cache enter final image.
7. Run image vulnerability/basic metadata inspection if tooling exists.
8. Stop for review.

**Suggested commit:** `build: package Paykit Server containers`

### Task 6: Add local closed config and Compose contract documentation

**Objective:** Supply exact config consumed by sibling Compose without weakening production schema.

**Files:**
- Document the generated ignored `.local/paykit-server/config.toml` contract; do not commit a trusted-key placeholder as runnable local config
- Modify: `paykit-server/tests/config.rs`
- Modify: `README.md`

**TDD sequence:**
1. Add parser test for exact local config shape.
2. Test exact generated shape: listen `0.0.0.0:3001`, allowed origin `http://localhost:8080`, Pubky `testnet`, Bitcoin `regtest`, Electrum `tcp://fulcrum:50001`, receiver `bitkit/server` matching the fixed companion capability, priority `["bitkit"]`, Electrum poll `1s`, and outbox poll `500ms`.
3. Keep database URL/master key environment-only.
4. Keep trusted Locks public key canonical and generated from actual local Lock Server identity.
5. Verify unknown keys remain rejected.
6. Stop for review.

**Suggested commit:** `dev: add local Paykit configuration`

### Task 7: Verify full service composition

**Objective:** Prove the actual Paykit image against PostgreSQL, Pubky testnet, regtest Electrum, Locks signed calls, and restart.

**Files:**
- Add/modify tests only where a reusable black-box check is warranted in `paykit-server-e2e/tests/`
- Modify: `docs/live-adapter-smoke.md` or add `docs/local-locks-compose.md`

**Verification sequence:**
1. Start sibling Locks Compose stack.
2. Check `GET /health/live`, `GET /health/ready`, and metrics without leaking identifiers.
3. Complete standard companion setup through helper and verify marker read-back + durable Creator state.
4. Restart Paykit Server and verify Creator rehydration.
5. Accept one signed Locks invoice, deliver one Payment Request to reader helper, observe the exact regtest mempool output, report matching `detected` status for Locks' zero-confirmation gate, and optionally verify finality after six blocks.
6. Exercise retry when Electrum is temporarily unavailable.
7. Run workspace tests, PostgreSQL tests, Clippy, formatting, Docker build, and `git diff --check`.
8. Stop for final manual review.

**Suggested commit:** `test: verify local Locks Paykit workflow`

## Final verification matrix

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
docker buildx build --load \
  --build-context paykit-lib='https://github.com/pubky/paykit-rs.git#v0.1.0-rc48:paykit-lib' \
  --build-context paykit-sdk='https://github.com/pubky/paykit-rs.git#v0.1.0-rc48:paykit-sdk' \
  --build-context locks='https://github.com/pubky/locks.git#b3a054fcdfc5e4bc21b6daa2f9e48138bc6375c4' \
  -f Dockerfile.local -t paykit-server:local .
git diff --check
```

The corresponding Locks integration must add executable wrapper commands that load ignored generated credentials and run explicit `TEST_DATABASE_URL` suites plus the Compose smoke without printing secrets.
