#!/bin/bash
set -euo pipefail

URL=https://ftp.ripe.net/ripe/asnames/asn.txt

TMP=$(mktemp)
trap 'rm -f $TMP' EXIT
SCRIPT_DIR=$(dirname "$0")
OUTPUT_DIR="$SCRIPT_DIR/../raw/atlas"
OUTPUT_SYMLINK="$OUTPUT_DIR/latest-asn-names.txt"
OUTPUT_FILE_NAME=$(date --utc +%Y%m%d-asn-names.txt)
OUTPUT_FILE="$OUTPUT_DIR/$OUTPUT_FILE_NAME"

if [ ! -d "$OUTPUT_DIR" ]
then
    echo "Error: Failed to find output directory: $OUTPUT_DIR"
    exit 1
fi

echo "Downloading $URL"
curl "$URL" -o "$TMP"

if [ -f "$OUTPUT_SYMLINK" ]
then
    echo "Comparing with existing file..."
    if diff "$TMP" "$OUTPUT_SYMLINK"
    then
        echo "Existing file is already up to date."
        exit 0
    fi
    rm "$OUTPUT_SYMLINK"
fi

mv "$TMP" "$OUTPUT_FILE"
ln -s "$OUTPUT_FILE_NAME" "$OUTPUT_SYMLINK"
