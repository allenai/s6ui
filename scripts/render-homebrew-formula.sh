#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEMPLATE_FILE="$PROJECT_ROOT/packaging/homebrew/s6ui.rb.in"
MANIFEST_FILE="$PROJECT_ROOT/Cargo.toml"

usage() {
    echo "Usage: $0 --url <url> --sha256 <sha256> --output <output>" >&2
    exit 1
}

if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "Error: formula template not found at $TEMPLATE_FILE" >&2
    exit 1
fi

if [ ! -f "$MANIFEST_FILE" ]; then
    echo "Error: Cargo.toml not found at $MANIFEST_FILE" >&2
    exit 1
fi

URL=""
SHA256=""
OUTPUT=""

while [ $# -gt 0 ]; do
    case "$1" in
        --url)
            URL="${2:-}"
            shift 2
            ;;
        --sha256)
            SHA256="${2:-}"
            shift 2
            ;;
        --output)
            OUTPUT="${2:-}"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

if [ -z "$URL" ] || [ -z "$SHA256" ] || [ -z "$OUTPUT" ]; then
    usage
fi

VERSION=$(sed -nE 's/^version = "([^"]+)"/\1/p' "$MANIFEST_FILE" | head -1)
if [ -z "$VERSION" ]; then
    echo "Error: failed to parse version from $MANIFEST_FILE" >&2
    exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"
sed \
    -e "s|__S6UI_URL__|$URL|g" \
    -e "s|__S6UI_SHA256__|$SHA256|g" \
    -e "s|__S6UI_VERSION__|$VERSION|g" \
    "$TEMPLATE_FILE" > "$OUTPUT"
