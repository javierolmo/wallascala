#!/bin/bash
# Script to compare two semantic versions and check if version has been incremented

set -e

OLD_VERSION="$1"
NEW_VERSION="$2"

if [ -z "$OLD_VERSION" ] || [ -z "$NEW_VERSION" ]; then
    echo "Error: Both old and new versions must be provided" >&2
    echo "Usage: $0 <old_version> <new_version>" >&2
    exit 1
fi

# Function to compare semantic versions
# Returns 0 if v2 > v1, 1 if v2 <= v1
compare_semver() {
    local v1=$1
    local v2=$2
    
    # Extract base version without prerelease/build metadata
    local v1_base=$(echo "$v1" | sed -E 's/^([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
    local v2_base=$(echo "$v2" | sed -E 's/^([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
    
    # Split versions into components
    IFS='.' read -r v1_major v1_minor v1_patch <<< "$v1_base"
    IFS='.' read -r v2_major v2_minor v2_patch <<< "$v2_base"
    
    # Compare major version
    if [ "$v2_major" -gt "$v1_major" ]; then
        return 0
    elif [ "$v2_major" -lt "$v1_major" ]; then
        return 1
    fi
    
    # Compare minor version
    if [ "$v2_minor" -gt "$v1_minor" ]; then
        return 0
    elif [ "$v2_minor" -lt "$v1_minor" ]; then
        return 1
    fi
    
    # Compare patch version
    if [ "$v2_patch" -gt "$v1_patch" ]; then
        return 0
    else
        return 1
    fi
}

if compare_semver "$OLD_VERSION" "$NEW_VERSION"; then
    echo "✓ Version has been incremented: $OLD_VERSION -> $NEW_VERSION"
    exit 0
else
    echo "Error: Version has not been incremented!" >&2
    echo "  Current version: $OLD_VERSION" >&2
    echo "  New version:     $NEW_VERSION" >&2
    exit 1
fi
