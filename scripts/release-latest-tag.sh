#!/bin/sh
# Decide whether a release tag should move the Docker :latest tag.
#
# Usage: sh scripts/release-latest-tag.sh <tag>
#
# Run inside a git checkout that has every tag (the publish workflow checks
# out with fetch-depth: 0). Only the newest stable vX.Y.Z tag gets
# latest=true; prereleases and older stable tags get latest=false. The result
# is appended to "$GITHUB_OUTPUT" when that variable is set.
set -eu

if [ "$#" -ne 1 ] || [ -z "$1" ]; then
    echo "usage: sh scripts/release-latest-tag.sh <tag>" >&2
    exit 2
fi

tag=$1
stable='^v[0-9]+\.[0-9]+\.[0-9]+$'
newest=''
latest=false

if printf '%s\n' "$tag" | grep -Eq "$stable"; then
    # Version-sort newest first and keep the first stable tag. awk prints
    # nothing (and still succeeds) when no stable tag exists. The pattern is
    # passed through the environment because awk -v would eat its backslashes.
    newest=$(git tag -l --sort=-v:refname 'v*' |
        STABLE=$stable awk '$0 ~ ENVIRON["STABLE"] && !found { print; found = 1 }')
    if [ "$tag" = "$newest" ]; then
        latest=true
    else
        echo "::warning::$tag is not the newest stable tag ('${newest:-none found}'), so :latest was left unchanged"
    fi
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "latest=$latest" >>"$GITHUB_OUTPUT"
fi
echo "Tag $tag: newest stable tag=${newest:-n/a}, latest=$latest"
