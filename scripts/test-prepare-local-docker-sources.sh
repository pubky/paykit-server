#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-.}/paykit-local-sources.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cat >"$tmp/Cargo.toml" <<'EOF'
[workspace.dependencies]
locks-core = { git = "https://github.com/pubky/locks.git", tag = "v0.1.1-rc1" }
paykit-lib = { git = "https://github.com/pubky/paykit-rs.git", rev = "f0dd83ddfd8246c31e5f761e7ea7ffca08b735b9" }
paykit-sdk = { git = "https://github.com/pubky/paykit-rs.git", rev = "f0dd83ddfd8246c31e5f761e7ea7ffca08b735b9" }
EOF

cat >"$tmp/Cargo.lock" <<'EOF'
[[package]]
name = "locks-core"
version = "0.1.0"
source = "git+https://github.com/pubky/locks.git?tag=v0.1.1-rc1#2874fae2308b8070e034d7bbc68df7791c8feded"

[[package]]
name = "paykit-sdk"
version = "0.1.1-rc4"
source = "git+https://github.com/pubky/paykit-rs.git?rev=f0dd83ddfd8246c31e5f761e7ea7ffca08b735b9#f0dd83ddfd8246c31e5f761e7ea7ffca08b735b9"
EOF

sh "$root/scripts/prepare-local-docker-sources.sh" \
    "$tmp/Cargo.toml" "$tmp/Cargo.lock"

grep -Fx 'locks-core = { path = "/build/locks/locks-core" }' "$tmp/Cargo.toml"
grep -Fx 'paykit-lib = { path = "/build/paykit-rs/paykit-lib" }' "$tmp/Cargo.toml"
grep -Fx 'paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }' "$tmp/Cargo.toml"
! grep -Fq 'git+https://github.com/pubky/locks.git?tag=v0.1.1-rc1' "$tmp/Cargo.lock"
! grep -Fq 'git+https://github.com/pubky/paykit-rs.git' "$tmp/Cargo.lock"
