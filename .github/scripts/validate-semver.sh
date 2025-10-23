#!/bin/bash
# Script to validate if a version follows semantic versioning format

set -e

VERSION="$1"

if [ -z "$VERSION" ]; then
    echo "Error: No version provided" >&2
    exit 1
fi

# Semver regex pattern
# Supports: X.Y.Z, X.Y.Z-prerelease, X.Y.Z+build, X.Y.Z-prerelease+build
SEMVER_REGEX='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?(\+([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'

if [[ ! "$VERSION" =~ $SEMVER_REGEX ]]; then
    echo "Error: Version '$VERSION' does not follow semantic versioning format" >&2
    echo "Expected format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]" >&2
    exit 1
fi

echo "Version '$VERSION' is valid semver"
