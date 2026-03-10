#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MANIFEST_FILE="$PROJECT_ROOT/Cargo.toml"

if [ ! -f "$MANIFEST_FILE" ]; then
    echo "Error: Cargo.toml not found at $MANIFEST_FILE"
    exit 1
fi

PACKAGE_VERSION=$(sed -nE 's/^version = "([^"]+)"/\1/p' "$MANIFEST_FILE" | head -1)

if [ -z "$PACKAGE_VERSION" ]; then
    echo "Error: Could not parse version from $MANIFEST_FILE"
    exit 1
fi

VERSION="v${PACKAGE_VERSION}"

echo "Version from Cargo.toml: $VERSION"

# Check if tag already exists
if git rev-parse "$VERSION" >/dev/null 2>&1; then
    echo "Error: Tag $VERSION already exists"
    exit 1
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "Error: You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

echo "Creating tag $VERSION..."
git tag -a "$VERSION" -m "Release $VERSION"

echo ""
echo "Tag $VERSION created successfully!"
echo ""
echo "Pushing to github"

git push origin $VERSION

echo ""
echo "Done! Please check that CI ran successfully"
