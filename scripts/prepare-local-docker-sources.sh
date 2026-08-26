#!/bin/sh
set -eu

manifest=${1:-Cargo.toml}
lockfile=${2:-Cargo.lock}

sed -i \
    -e 's#locks-core = { git = "https://github.com/pubky/locks.git", rev = "df5ea1b6d8dcdec3a9b5a915c3f57bca69d75c8a" }#locks-core = { path = "/build/locks/locks-core" }#' \
    -e 's#paykit-lib = { git = "https://github.com/pubky/paykit-rs.git", tag = "v0.1.0-rc47" }#paykit-lib = { path = "/build/paykit-rs/paykit-lib" }#' \
    -e 's#paykit-sdk = { git = "https://github.com/pubky/paykit-rs.git", tag = "v0.1.0-rc47" }#paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }#' \
    "$manifest"

sed -i \
    -e '/source = "git+https:\/\/github.com\/pubky\/locks.git?rev=df5ea1b6d8dcdec3a9b5a915c3f57bca69d75c8a#df5ea1b6d8dcdec3a9b5a915c3f57bca69d75c8a"/d' \
    -e '/source = "git+https:\/\/github.com\/pubky\/paykit-rs.git?tag=v0.1.0-rc47#ba528c71d9ae837ea88380a92c7fc2a38e873582"/d' \
    "$lockfile"

grep -Fx 'locks-core = { path = "/build/locks/locks-core" }' "$manifest"
grep -Fx 'paykit-lib = { path = "/build/paykit-rs/paykit-lib" }' "$manifest"
grep -Fx 'paykit-sdk = { path = "/build/paykit-rs/paykit-sdk" }' "$manifest"
! grep -Eq 'git = "(ssh://git@github\.com/pubky/locks|https://github\.com/pubky/(locks|paykit-rs))' "$manifest"
! grep -Eq 'source = "git\+(ssh://git@github\.com/pubky/locks|https://github\.com/pubky/(locks|paykit-rs))' "$lockfile"
