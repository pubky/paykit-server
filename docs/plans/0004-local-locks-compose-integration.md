# Local Locks Compose Integration Correction Plan

> **For Hermes:** Use `subagent-driven-development` to implement this plan task-by-task only after the paired plan-only checkpoint is approved. Stop after each commit-sized task for manual review.

**Goal:** Remove the production-scoped opaque companion-handle design, restore the production Paykit setup shell to the approved Bitkit QR/deep-link shape, and retain a protocol-real auth-URL helper only as local-demo tooling.

**Architecture:** Production Paykit Server creates and retains the normal augmented `pubkyauth://` request, renders it as a Bitkit QR/deep link, receives the standard encrypted companion claim, and completes setup without any demo handle route. A Paykit-owned Cargo example substitutes for Bitkit in local Compose by consuming an operator-pasted auth URL plus Creator/xpub inputs over strict stdin and invoking the canonical `paykit-sdk` companion-approval operation. `Dockerfile.local` is explicitly local/demo packaging and may build/copy that Cargo example; normal Paykit package binaries and production server routes do not include it.

**Tech stack:** Rust 1.91.1, Axum, `paykit-sdk`, PostgreSQL/SQLx, Docker multi-stage builds, Pubky static testnet, Bitcoin Core regtest, BDK Electrum.

**Sibling plan:** `../../../../Pubky/locks-public/docs/plans/2026-08-31-paykit-demo-auth-url-helper-correction.md` in the standalone Locks repository.

---

## Requirement provenance

### EXPLICIT

- The companion helper is local-demo tooling; the demo may trust the operator-supplied authorization URL.
- Production Paykit Server must expose no companion handle endpoint, state/lifecycle, handle UI, or normal helper binary target.
- Production setup UI must match Paykit commit `06d227444f186a24d82b1f15d8c8b83d4e535ce2`: desktop Bitkit QR, touch-device `Continue with Bitkit` deep link, parent-owned modal chrome, no handle, no URL text, and no helper command inside the iframe.
- The local helper must remain protocol-real and use canonical `paykit-sdk`; no manual server claim route or database bypass is allowed.
- The demo accepts requester-key (`cpk`), relay, and encryption-secret substitution risk from the pasted URL.
- The helper still validates URL parseability, exact Paykit client ID, exact capabilities `/pub/paykit/v0/bitkit/server/:rw,/pub/paykit/v0/private/bitkit/server/:rw`, and exact `watch-only-account-v1` companion-claim type.
- The helper remains prompt/strict-stdin driven; auth URL, Creator secret, and xpub do not enter argv or `postMessage`, and helper output remains coarse.
- Paykit config adds `[setup].log_authorization_url`, default `false`; local Compose sets it to `true`.
- When URL logging is enabled, emit one stable URL-bearing line when a setup flow starts; the local operator owns log retention.
- Production setup HTML/responses retain `Cache-Control: no-store` even though the visual reference predates that header.
- Already-pushed handle work remains in feature-branch history; correction is a new commit and PRs rely on squash merge for clean master history.

### AUTHORITATIVE SOURCE

- Paykit setup appearance: commit `06d227444f186a24d82b1f15d8c8b83d4e535ce2`, `paykit-server/src/http/setup.rs`.
- Lock Server connect-shell appearance: Locks PR-head commit `3bcd197bd171e63097041178c3fa8663dd08dd57` (merged equivalent `808ee46`).
- Companion protocol: `paykit-rs` `v0.1.0-rc48`, `specs/pubky-auth-companion-claims.md`.
- Canonical companion approval: `paykit-sdk/src/pubky_session/companion_claim.rs` at `v0.1.0-rc48`.
- Server claim validation: `paykit-server/src/bitkit_claim.rs` and `paykit-server/src/real_setup.rs`.
- Paired Locks correction plan named above.

### CONSTRAINTS

- Branch `fix/locks-core-update-follow-up` already contains pushed commit `61be6be` implementing opaque handles.
- Package release metadata is already `0.1.0-rc2`; no published Paykit `v*` tag exists.
- `Dockerfile.local` is explicitly local/demo-only and currently supplies helper binaries to the sibling Locks demo image.
- The iframe must never receive xpub/tpub, Creator secret, or companion payload bytes.
- `POST /setup/{flow_id}/complete`, standard relay receipt, claim verification, durable Creator installation, and secret-free callback behavior remain production paths.
- Reader helper, payment request, Bitcoin observation, persistence, and readiness work are outside this correction except where shared Docker commands need explicit binary selection.

### ACCEPTED WEAKENED DEMO BOUNDARY

The local helper trusts URL provenance. A modified URL may substitute requester key, relay, or encryption secret. This is accepted only because the helper is built and invoked as controlled local-demo tooling. It is not a production authentication guarantee and must be stated in operator documentation.

### UNRESOLVED

None.

---

## Current-state correction audit

| Current handle implementation | Required corrected state |
| --- | --- |
| `Flow` retains companion handle hash | No handle material in setup state |
| `StartedFlow` returns raw handle | No handle field |
| `POST /setup/companion-auth-request` | Route and DTOs removed |
| iframe renders handle and helper command | Reference QR/deep-link shell only |
| `src/bin/paykit-companion-auth.rs` | Cargo example under `paykit-server/examples/` |
| helper stdin uses `companion_handle` | helper stdin uses `auth_url` |
| helper fetches URL with Reqwest | no helper HTTP client or `PAYKIT_SERVER_URL` |
| URL never logs | one local-demo-only log line behind default-false config |
| handle-specific HTTP/adversarial tests | URL validation and demo-scope tests |

Implementation audit (current uncommitted tree): Tasks 2–4 are implemented and
were accepted for this branch checkpoint. Task 5 documentation/config-example
alignment is implemented pending manual review. Task 6 full Paykit,
cross-repository, image, and live Compose verification remains pending; this
audit is not self-approval.

---

### Task 1: Paired plan-only correction checkpoint

**Objective:** Lock the reversal in both repositories before changing code.

**Files:**
- Modify: `docs/plans/0004-local-locks-compose-integration.md`
- Create in Locks: `docs/plans/2026-08-31-paykit-demo-auth-url-helper-correction.md`

**Steps:**
1. Record requirement provenance, accepted weakened boundary, exact UI commits, config gate, helper location, history strategy, and release order in both plans.
2. Link each plan to the other and assign every changed file to one repository owner.
3. Search both plans for stale `companion_handle`, handle endpoint, server URL exchange, and “never log URL” decisions; allow those terms only in correction/audit context.
4. Run `git diff --check` in both repositories.
5. Stop for manual plan review.

**Suggested commit:** `docs: correct local companion helper plan`

---

### Task 2: Remove production companion-handle surface and restore setup shell

**Objective:** Return production setup to the standard Bitkit URL/QR flow with no demo handle API.

**Files:**
- Modify: `paykit-server/src/setup.rs`
- Modify: `paykit-server/src/http/setup.rs`
- Modify: `paykit-server/src/config.rs`
- Modify: `paykit-server/src/server.rs`
- Modify: `paykit-server/tests/setup.rs`
- Modify: `paykit-server/tests/config.rs`
- Modify: `paykit-server/tests/runtime.rs` or the nearest production-constructor test that proves config-to-service plumbing

**RED sequence:**
1. Add route regression asserting `/setup/companion-auth-request` is not mounted.
2. Add setup-state regression proving `StartedFlow` and `Flow` expose/store no companion handle material.
3. Add exact shell regression for the `06d2274` shape: desktop QR, touch deep link, no handle, no auth URL text, no helper command, no xpub, unchanged polling/callback, exact CSP, and `no-store`.
4. Add config regressions: absent `log_authorization_url` defaults false; explicit true is accepted; unknown setup keys still fail.
5. Add logging seam regression proving URL-bearing event is emitted once only when enabled and never when disabled. Use an injected/testable event sink or narrowly scoped tracing capture; do not assert raw URL through general logs.
6. Add a production-constructor regression proving parsed `setup.log_authorization_url` reaches the `SetupService` used by the mounted setup router; a workspace compile alone is not sufficient evidence of value propagation.
7. Run focused tests and observe expected RED failures caused by current handle route/state/UI and missing config field.

**GREEN sequence:**
1. Remove handle generation, hashing, lookup result types, fields, terminal invalidation code, route DTOs, and route mount.
2. Restore the visual shell structure/CSS from `06d2274` while preserving current `no-store`, exact `frame-ancestors`, polling retry set, target origin, state, and coarse callback.
3. Add `SetupConfig::log_authorization_url: bool` with serde default false.
4. Thread the flag through `paykit-server/src/server.rs` into setup construction and emit one explicitly labeled local-demo URL log event at flow start only when enabled.
5. Run focused setup/config tests to GREEN.
6. Run `cargo check --locked --workspace --all-targets` to catch constructor/config fallout.

**Suggested commit:** `fix(setup): restore production Bitkit setup boundary`

---

### Task 3: Relocate and simplify the local companion helper

**Objective:** Keep the canonical companion producer as demo-only Cargo example using pasted auth URL.

**Files:**
- Delete: `paykit-server/src/bin/paykit-companion-auth.rs`
- Create: `paykit-server/examples/paykit-companion-auth.rs`
- Modify or replace: `paykit-server/tests/companion_auth_cli.rs`
- Modify: `Cargo.toml`
- Modify: `paykit-server/Cargo.toml`
- Modify: `Cargo.lock`

**Closed v1 stdin contract:**

```json
{"version":1,"auth_url":"pubkyauth://...","creator_secret":"<base64url-32>","account_xpub":"tpub...","account_index":0}
```

**RED sequence:**
1. Structure the Cargo example around an example-local callable runner that accepts injected args, stdin reader, stdout/stderr writers, and the canonical async approval dependency; keep this seam inside `examples/paykit-companion-auth.rs`, not the production library.
2. Add Cargo-example unit tests (`cargo test -p paykit-server --example paykit-companion-auth`) for exact closed schema, unknown/missing fields, version, bounded canonical auth URL, Creator secret, tpub/network/depth/account index, empty/extra argv rejection, and exact success/failure output through injected I/O.
3. Add URL contract tests requiring canonical SDK parse, exact `app.paykit.server`, exact literal capabilities `/pub/paykit/v0/bitkit/server/:rw,/pub/paykit/v0/private/bitkit/server/:rw`, and exact `watch-only-account-v1` query/type.
4. Add tests documenting accepted provenance gap: helper does not compare `cpk`, relay, or secret against server state.
5. Move canonical relay/approval ordering evidence into example-local async tests around the callable runner; prove companion delivery is attempted before grant approval.
6. Keep process deadline/kill/reap/stdout-stderr-cap assertions in the Locks Node wrapper tests, where the bounded child-process owner remains. Do not duplicate those process-supervision tests in the Rust example.
7. Reserve packaged-process checks for `Dockerfile.local`: empty stdin, extra argv, exact coarse errors, and executable presence. Full Compose E2E proves the built example performs the real protocol flow.
8. Verify URL/secret/xpub never appear in helper output or `Debug` paths.
9. Observe RED against current handle schema and HTTP exchange.

**GREEN sequence:**
1. Move helper to Cargo examples and replace `companion_handle` with `auth_url`.
2. Remove Reqwest URL exchange, redirect/MIME/cache/body-bound code, `PAYKIT_SERVER_URL`, and handle canonicalization.
3. Use canonical Paykit/Pubky parsers and existing xpub/claim helpers; do not duplicate signing/encryption/relay logic.
4. Preserve strict stdin, bounded process input, coarse output, creator-secret handling, and claim-before-grant ordering.
5. Remove direct Reqwest dependency if no other production target uses it; regenerate lockfile.
6. Run Cargo example tests and relevant crate tests to GREEN.

**Suggested commit:** `fix(demo): trust operator-supplied companion URL`

---

### Task 4: Keep helper packaging local-demo-only

**Objective:** Build the Cargo example only in `Dockerfile.local` and keep normal package binaries clean.

**Files:**
- Modify: `Dockerfile.local`
- Modify: `docs/local-locks-demo.md`
- Modify: `README.md`

**Verification sequence:**
1. Replace broad `cargo build ... --bins` assumptions with explicit server/reader binary targets plus `--example paykit-companion-auth`.
2. Install the example artifact from `target/release/examples/paykit-companion-auth` into the local-demo runtime image for the Locks creator image to copy.
3. Keep image labels explicitly local/demo-only.
4. Keep build-time empty-stdin helper rejection smoke and server startup-contract smoke.
5. Use `cargo metadata --no-deps --format-version 1` to assert `paykit-companion-auth` target kind is `example`, never `bin`.
6. Use a fresh isolated `CARGO_TARGET_DIR` to run `cargo build -p paykit-server --bins`; assert no helper artifact exists there so stale artifacts cannot create false evidence.
7. In a separate isolated target directory, build `--example paykit-companion-auth` and assert the artifact is under `target/release/examples/` (or the selected profile-equivalent path).
8. Verify ordinary server routes/package binaries contain no helper or handle surface, while `Dockerfile.local` explicitly builds and installs `target/release/examples/paykit-companion-auth`.
9. Build exact public/local contexts with Paykit Rust `v0.1.0-rc48` and Locks `v0.1.0-rc1`.

**Suggested commit:** `build(demo): package companion helper example`

---

### Task 5: Align current documentation and security statements

**Objective:** Remove handle claims and document the explicitly weakened local-demo boundary without changing production protocol claims.

**Files:**
- Modify: `README.md`
- Modify: `docs/local-locks-demo.md`
- Modify: `config/paykit-server.example.toml`
- Modify: `docs/plans/0001-receiver-only-prototype-design.md`
- Modify: `docs/plans/0004-local-locks-compose-integration.md` implementation audit/status

**Steps:**
1. Document production Bitkit QR/deep-link flow and no helper/handle production surface.
2. Document `[setup].log_authorization_url = true` as local-demo-only, bearer-secret logging with operator-owned retention.
3. Add `log_authorization_url = false` to `config/paykit-server.example.toml`; state that Locks-generated local config is the sole planned `true` setting.
4. Reconcile the active plan 0001 claim that setup URLs never enter logs: default/production remains no-log, while explicit local-demo config is an accepted exception with operator-owned retention.
5. Document strict helper stdin and accepted `cpk`/relay/secret substitution risk.
6. Remove `companion_handle`, handle exchange endpoint, helper `PAYKIT_SERVER_URL`, and server-provenance claims from active docs.
7. Search current docs/tests/source for stale terms; historical commit messages need not be rewritten.
8. Run Markdown/stale-term checks and `git diff --check`.

**Suggested commit:** fold into the correction commit unless repository policy requires docs-only separation.

---

### Task 6: Full Paykit and cross-repository verification

**Objective:** Prove corrected production and demo boundaries through tests, images, and live Compose.

**Paykit verification:**

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked -p paykit-server -- --test-threads=1
cargo clippy --locked --workspace --all-targets -- -D warnings
git diff --check
```

Run PostgreSQL 16 E2E with an isolated `TEST_DATABASE_URL`; preserve caller-provided values, print no connection string, and verify zero leaked temporary databases.

**Image verification:**

```bash
docker buildx build --load \
  --file Dockerfile.local \
  --build-context paykit-lib='https://github.com/pubky/paykit-rs.git#v0.1.0-rc48:paykit-lib' \
  --build-context paykit-sdk='https://github.com/pubky/paykit-rs.git#v0.1.0-rc48:paykit-sdk' \
  --build-context locks='https://github.com/pubky/locks.git#v0.1.0-rc1' \
  --tag paykit-server:auth-url-helper-correction .
```

**Cross-repository acceptance:**
1. Build Locks creator/reader images from the exact corrected local Paykit image.
2. Start exact `compose.paykit-local-demo.yaml` stack from retained state and from documented fresh reset.
3. Verify Locks, Paykit live/ready, creator, and reader HTTP endpoints.
4. Start setup and verify Paykit iframe matches `06d2274`, with no handle/helper UI and `no-store`.
5. With local logging enabled, copy latest URL-bearing log line, run host wrapper, submit xpub/index, and complete normal setup.
6. Verify production-shaped path with QR/deep link remains unchanged and callback/readiness stays secret-free.
7. Obtain independent Paykit, Locks, and cross-repository approval on exact final trees.

**Suggested correction commit:** `fix(auth): keep companion helper demo-only`

---

## Merge and release order

1. Add correction commits to existing pushed Paykit and Locks branches; do not rewrite history.
2. Push both branches; do not merge Locks first.
3. Squash-merge Paykit after CI/review so handle implementation does not enter master history.
4. Release Paykit `v0.1.0-rc2` from exact merged commit.
5. Update Locks branch default Paykit context/validators/docs to immutable `v0.1.0-rc2` while retaining explicit local worktree override.
6. Squash-merge Locks after public-context build and live Compose verification.
7. Prepare and release Locks `v0.1.0-rc2` separately.
8. Do not create an artificial Paykit↔Locks release loop; Paykit rc2 may continue consuming Locks Core rc1 unless Core behavior actually changes.