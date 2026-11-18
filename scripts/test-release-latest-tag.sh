#!/bin/sh
# Regression test for scripts/release-latest-tag.sh and the publish workflow's
# concurrency group, which together keep :latest from moving backwards when
# release tags are pushed close together.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
script="$root/scripts/release-latest-tag.sh"
workflow="$root/.github/workflows/docker.yml"
tmp=$(mktemp -d "${TMPDIR:-.}/paykit-release-latest.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
# Without TMPDIR the path is relative, and expect() runs the script from
# inside a throwaway repo, so make every derived path absolute.
tmp=$(CDPATH='' cd -- "$tmp" && pwd)

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# new_repo NAME: create a throwaway repository with one commit and use it.
new_repo() {
    repo="$tmp/$1"
    git init -q "$repo"
    git -C "$repo" -c user.name=test -c user.email=test@example.invalid \
        -c commit.gpgSign=false commit -q --no-verify --allow-empty -m init
}

add_tag() {
    git -C "$repo" -c tag.gpgSign=false tag "$1"
}

# expect TAG LATEST WARNING: run the real script for TAG in $repo, check the
# latest output and whether a ::warning:: was printed (yes or no). The
# publish runs are serialized, so the last run that returned true owns :latest.
expect() {
    output="$tmp/github-output"
    log="$tmp/stdout"
    : >"$output"
    (cd "$repo" && GITHUB_OUTPUT="$output" sh "$script" "$1") >"$log"
    latest=$(sed -n 's/^latest=//p' "$output")
    [ "$latest" = "$2" ] || fail "$1: latest=$latest, want $2"
    if [ "$3" = yes ]; then
        grep -Fq "::warning::$1 is not the newest stable tag" "$log" ||
            fail "$1: expected a ::warning::"
    elif grep -Fq '::warning::' "$log"; then
        fail "$1: unexpected ::warning::"
    fi
    if [ "$latest" = true ]; then
        owner=$1
    fi
}

# a. v1.1.0 publishes before v1.2.0 is tagged, then v1.2.0 publishes.
new_repo older-first
owner=''
add_tag v1.1.0
expect v1.1.0 true no
add_tag v1.2.0
expect v1.2.0 true no
[ "$owner" = v1.2.0 ] || fail "older first: :latest owner is $owner"
echo "PASS a: older stable tag first, :latest ends on v1.2.0"

# b. Both tags exist and v1.2.0 publishes before v1.1.0.
new_repo newer-first
owner=''
add_tag v1.1.0
add_tag v1.2.0
expect v1.2.0 true no
expect v1.1.0 false yes
[ "$owner" = v1.2.0 ] || fail "newer first: :latest owner is $owner"
echo "PASS b: newer stable tag first, :latest stays on v1.2.0"

# c. A backport patch tagged after a newer minor release.
add_tag v1.1.1
expect v1.1.1 false yes
echo "PASS c: backport v1.1.1 leaves :latest alone"

# d. Versions compare numerically, not as strings.
new_repo ordering
add_tag v0.9.9
add_tag v0.10.0
expect v0.10.0 true no
expect v0.9.9 false yes
echo "PASS d: v0.10.0 is newer than v0.9.9"

# e. A prerelease never moves :latest and is not warned about.
repo="$tmp/newer-first"
add_tag v1.3.0-rc1
expect v1.3.0-rc1 false no
echo "PASS e: prerelease v1.3.0-rc1 leaves :latest alone"

# f. A stable tag that is not in the tag list (only an rc exists).
new_repo rc-only
add_tag v2.0.0-rc1
expect v2.0.0 false yes
grep -Fq "('none found')" "$log" || fail "v2.0.0: expected 'none found'"
echo "PASS f: missing stable tag reports none found"

# g. The tag argument is required.
if (cd "$repo" && sh "$script") >/dev/null 2>&1; then
    fail "missing argument: expected a non-zero exit"
fi
echo "PASS g: missing argument is rejected"

# h. The publish workflow serializes every release run and uses the script.
concurrency=$(awk '
    /^concurrency:/ { in_block = 1; next }
    in_block && /^[^ ]/ { exit }
    in_block { print }
' "$workflow")
for line in 'group: docker-publish' 'cancel-in-progress: false' 'queue: max'; do
    printf '%s\n' "$concurrency" | grep -Eq "^[[:space:]]+$line\$" ||
        fail "docker.yml: top-level concurrency is missing '$line'"
done
# shellcheck disable=SC2016 # a literal GitHub expression, not a shell one
grep -Fq 'latest: ${{ steps.latest.outputs.latest }}' "$workflow" ||
    fail "docker.yml: build step does not use steps.latest.outputs.latest"
awk '
    /id: latest$/ { in_step = 1; next }
    in_step && /- name:/ { exit }
    in_step && /run: sh scripts\/release-latest-tag\.sh "\$TAG"$/ { found = 1 }
    END { exit !found }
' "$workflow" || fail "docker.yml: latest step does not run release-latest-tag.sh"
echo "PASS h: docker.yml serializes releases and runs release-latest-tag.sh"
