#!/usr/bin/env bash
# Copies the protocol definition and the golden files from a sqlite-remote-vfs checkout, the source of truth, into
# proto/. Then lints the copy and regenerates the Go code in internal/gen.
#
#   scripts/sync-proto.sh [path]          copy from path (default ../sqlite-remote-vfs) and regenerate
#   scripts/sync-proto.sh --check [path]  compare only, fail on any difference
set -euo pipefail

cd "$(dirname "$0")/.."

check=false
if [[ "${1:-}" == "--check" ]]; then
  check=true
  shift
fi
source_repo="${1:-../sqlite-remote-vfs}"

if [[ ! -f "$source_repo/proto/sqlite_remote/v1/sqlite_remote.proto" ]]; then
  echo "no sqlite-remote-vfs checkout at $source_repo" >&2
  exit 1
fi

if $check; then
  diff -r "$source_repo/proto/sqlite_remote" proto/sqlite_remote
  diff -r "$source_repo/proto/testdata" proto/testdata
  echo "proto/ matches $source_repo"
  exit 0
fi

rm -rf proto/sqlite_remote proto/testdata
mkdir -p proto
cp -R "$source_repo/proto/sqlite_remote" "$source_repo/proto/testdata" proto/

commit="$(git -C "$source_repo" rev-parse HEAD)"
if [[ -n "$(git -C "$source_repo" status --porcelain -- proto)" ]]; then
  commit="$commit plus uncommitted changes"
fi
echo "sqlite-remote-vfs $commit" > proto/SOURCE

go tool buf lint
go tool buf generate
echo "synced proto/ from sqlite-remote-vfs $commit"
