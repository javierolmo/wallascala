#!/bin/bash
# Script to extract version from pom.xml

set -e

POM_FILE="${1:-pom.xml}"

if [ ! -f "$POM_FILE" ]; then
    echo "Error: POM file not found: $POM_FILE" >&2
    exit 1
fi

# Extract version using grep and sed
VERSION=$(grep -m 1 "^\s*<version>" "$POM_FILE" | sed 's/.*<version>\(.*\)<\/version>.*/\1/' | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo "Error: Could not extract version from $POM_FILE" >&2
    exit 1
fi

echo "$VERSION"
