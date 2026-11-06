#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-.}/paykit-local-sources.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

cat >"$tmp/Cargo.toml" <<'EOF'
[workspace.dependencies]
locks-core = { git = "https://github.com/pubky/locks.git", tag = "v0.1.1-rc2" }
paykit-lib = { git = "https://github.com/pubky/paykit-rs.git", tag = "v0.1.1-rc6" }
paykit-sdk = { git = "https://github.com/pubky/paykit-rs.git", tag = "v0.1.1-rc6" }
EOF

cat >"$tmp/Cargo.lock" <<'EOF'
[[package]]
name = "locks-core"
version = "0.1.0"
source = "git+https://github.com/pubky/locks.git?tag=v0.1.1-rc2#3013d8c181a2cf7a44c216e9e2440445be87b3d5"

[[package]]
name = "paykit-lib"
version = "0.1.1-rc6"
source = "git+https://github.com/pubky/paykit-rs.git?tag=v0.1.1-rc6#9919198a8a810652945f7366defacae8c3135632"

[[package]]
name = "paykit-sdk"
version = "0.1.1-rc6"
source = "git+https://github.com/pubky/paykit-rs.git?tag=v0.1.1-rc6#9919198a8a810652945f7366defacae8c3135632"
EOF

sh "$root/scripts/prepare-local-docker-sources.sh" \
    "$tmp/Cargo.toml" "$tmp/Cargo.lock"

grep -Fx 'locks-core = { path = "/build/locks/locks-core" }' "$tmp/Cargo.toml"
grep -Fx 'paykit-lib = { path = "/build/paykit-rs/paykit-lib" }' "$tmp/Cargo.toml"
grep -Fx 'paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }' "$tmp/Cargo.toml"
! grep -Fq 'git+https://github.com/pubky/locks.git?tag=v0.1.1-rc2' "$tmp/Cargo.lock"
! grep -Fq 'git+https://github.com/pubky/paykit-rs.git?tag=v0.1.1-rc6' "$tmp/Cargo.lock"
