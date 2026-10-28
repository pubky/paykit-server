#!/bin/sh
set -eu

manifest=${1:-Cargo.toml}
lockfile=${2:-Cargo.lock}

sed -i \
    -e 's#locks-core = { git = "https://github.com/pubky/locks.git", tag = "v0.1.1-rc1" }#locks-core = { path = "/build/locks/locks-core" }#' \
    -e 's#paykit-lib = "=0.1.0"#paykit-lib = { path = "/build/paykit-rs/paykit-lib" }#' \
    -e 's#paykit-sdk = "=0.1.0"#paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }#' \
    "$manifest"

sed -i \
    -e '/source = "git+https:\/\/github.com\/pubky\/locks.git?tag=v0.1.1-rc1#2874fae2308b8070e034d7bbc68df7791c8feded"/d' \
    "$lockfile"

grep -Fx 'locks-core = { path = "/build/locks/locks-core" }' "$manifest"
grep -Fx 'paykit-lib = { path = "/build/paykit-rs/paykit-lib" }' "$manifest"
grep -Fx 'paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }' "$manifest"
! grep -Eq 'git = "https://github\.com/pubky/paykit-rs' "$manifest"
! grep -Eq 'git = "https://github\.com/pubky/locks' "$manifest"
! grep -Eq 'source = "git\+https://github\.com/pubky/paykit-rs' "$lockfile"
! grep -Eq 'source = "git\+https://github\.com/pubky/locks' "$lockfile"
