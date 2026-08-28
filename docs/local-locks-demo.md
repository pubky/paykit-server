# Local Locks demo image

`Dockerfile.local` packages Paykit Server for the Locks Compose stack. It is local/demo packaging, not a production image.

## Build from public release sources

From a fresh anonymous clone of this repository, build with the Paykit release
and exact Locks revision selected in `Cargo.toml`:

```bash
docker buildx build --load \
  --build-context paykit-lib='https://github.com/pubky/paykit-rs.git#v0.1.0-rc47:paykit-lib' \
  --build-context paykit-sdk='https://github.com/pubky/paykit-rs.git#v0.1.0-rc47:paykit-sdk' \
  --build-context locks='https://github.com/pubky/locks.git#df5ea1b6d8dcdec3a9b5a915c3f57bca69d75c8a' \
  -f Dockerfile.local \
  -t paykit-server:local .
```

These contexts are anonymously reachable and reproducible. Update the URLs
together with the corresponding `Cargo.toml` pins; exact dependency-pin matches
make source drift fail closed.

## Build from local worktrees

For coordinated development, override the named contexts with local source
trees. This mode intentionally includes uncommitted source edits:

```bash
docker buildx build --load \
  --build-context paykit-lib=../paykit-rs/paykit-lib \
  --build-context paykit-sdk=../paykit-rs/paykit-sdk \
  --build-context locks=../../Pubky/locks \
  -f Dockerfile.local \
  -t paykit-server:local .
```

Crate-level `paykit-lib` and `paykit-sdk` contexts avoid transferring the Paykit
Rust workspace target directory and require no Docker-owned files in that
repository. The Locks context applies its existing source-tree exclusions.

Builder runs [`scripts/prepare-local-docker-sources.sh`](../scripts/prepare-local-docker-sources.sh) against copied manifests to resolve `paykit-lib`, `paykit-sdk`, and `locks-core` from named contexts. Committed Git dependency declarations and lockfile remain unchanged. No SSH agent or Cargo credentials are mounted. Exact dependency-pin matches make source drift fail closed.

## Runtime image

Final pinned Debian image:

- runs as unprivileged UID/GID `10001`;
- defaults to `/usr/local/bin/paykit-server` and exposes port `3001`;
- contains `/usr/local/bin/paykit-companion-auth` and `/usr/local/bin/paykit-reader-demo` for explicit Compose command/entrypoint overrides;
- adds CA certificates and three application binaries to pinned runtime base;
- contains no source tree, Cargo cache, runnable config, DB credentials, or application secrets.

Supply `PAYKIT_CONFIG`, `PAYKIT_DATABASE_URL`, and `PAYKIT_MASTER_KEY` at runtime. Mount generated ignored local config. The calling Compose definition owns mounts, environment values, infrastructure image pins, and helper command overrides.

## Generated local config contract

Locks orchestration generates `.local/paykit-server/config.toml` only after the local Lock Server identity exists. The directory is Git-ignored. It must replace the deliberately invalid `<ACTUAL_CANONICAL_LOCK_SERVER_PUBKY>` token below with the exact canonical `pubky...` value exposed by Lock Server as `credentials.lock_server_public_key`. This block is not runnable config.

```toml
[http]
listen_addr = "0.0.0.0:3001"

[locks]
trusted_public_key = "<ACTUAL_CANONICAL_LOCK_SERVER_PUBKY>"

[setup]
allowed_origins = ["http://localhost:8080"]

[paykit]
receiver_path = "bitkit/server"
receiver_path_priority = ["bitkit"]
network = "testnet"

[bitcoin]
network = "regtest"

[electrum]
endpoint = "tcp://fulcrum:50001"
poll_interval = "1s"

[outbox]
poll_interval = "500ms"
```

File contains no DB URL or master key. Compose supplies those only through `PAYKIT_DATABASE_URL` and `PAYKIT_MASTER_KEY`, then points `PAYKIT_CONFIG` at mounted generated file. Parser rejects unknown top-level keys, unknown section keys, non-canonical trusted keys, and secrets placed in TOML. Production schema and defaults remain unchanged.
