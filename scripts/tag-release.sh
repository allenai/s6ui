#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VERSION_FILE="$PROJECT_ROOT/src/version.h"

if [ ! -f "$VERSION_FILE" ]; then
    echo "Error: version.h not found at $VERSION_FILE"
    exit 1
fi

MAJOR=$(grep '#define VERSION_MAJOR' "$VERSION_FILE" | awk '{print $3}')
MINOR=$(grep '#define VERSION_MINOR' "$VERSION_FILE" | awk '{print $3}')
PATCH=$(grep '#define VERSION_PATCH' "$VERSION_FILE" | awk '{print $3}')

if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$PATCH" ]; then
    echo "Error: Could not parse version from $VERSION_FILE"
    exit 1
fi

VERSION="v${MAJOR}.${MINOR}.${PATCH}"

echo "Version from src/version.h: $VERSION"

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
echo "To push the tag and trigger the release workflow, run:"
echo "  git push origin $VERSION"
