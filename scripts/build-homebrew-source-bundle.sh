#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MANIFEST_FILE="$PROJECT_ROOT/Cargo.toml"

usage() {
    echo "Usage: $0 --output <tarball>" >&2
    exit 1
}

if [ ! -f "$MANIFEST_FILE" ]; then
    echo "Error: Cargo.toml not found at $MANIFEST_FILE" >&2
    exit 1
fi

OUTPUT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --output)
            OUTPUT="${2:-}"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

if [ -z "$OUTPUT" ]; then
    usage
fi

VERSION=$(sed -nE 's/^version = "([^"]+)"/\1/p' "$MANIFEST_FILE" | head -1)
if [ -z "$VERSION" ]; then
    echo "Error: failed to parse version from $MANIFEST_FILE" >&2
    exit 1
fi

TMPDIR_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_ROOT"' EXIT

BUNDLE_NAME="s6ui-${VERSION}-homebrew"
BUNDLE_DIR="$TMPDIR_ROOT/$BUNDLE_NAME"

mkdir -p "$BUNDLE_DIR"
mkdir -p "$(dirname "$OUTPUT")"

tar \
    --exclude=".git" \
    --exclude="target" \
    --exclude="dist" \
    --exclude=".DS_Store" \
    -C "$PROJECT_ROOT" \
    -cf - . | tar -xf - -C "$BUNDLE_DIR"

mkdir -p "$BUNDLE_DIR/.cargo"
(
    cd "$BUNDLE_DIR"
    cargo vendor --locked vendor > .cargo/config.toml
)

tar -C "$TMPDIR_ROOT" -czf "$OUTPUT" "$BUNDLE_NAME"
