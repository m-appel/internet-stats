#!/bin/bash
set -euo pipefail


REGIONS=(current au london singapore hk)

TMP=$(mktemp)
trap 'rm -f $TMP' EXIT
SCRIPT_DIR=$(dirname "$0")
OUTPUT_DIR="$SCRIPT_DIR/../raw/thyme"

if [ ! -d "$OUTPUT_DIR" ]
then
    echo "Error: Failed to find output directory: $OUTPUT_DIR"
    exit 1
fi

for R in "${REGIONS[@]}"
do
    OUTPUT_SYMLINK="$OUTPUT_DIR/latest-bgp-analysis-$R.txt"
    OUTPUT_FILE_NAME=$(date --utc "+%Y%m%d-bgp-analysis-$R.txt")
    OUTPUT_FILE="$OUTPUT_DIR/$OUTPUT_FILE_NAME"
    URL="https://thyme.apnic.net/$R/data-summary"
    echo "Downloading $URL"
    curl "$URL" -o "$TMP"

    if [ -f "$OUTPUT_SYMLINK" ]
    then
        echo "Comparing with existing file..."
        if diff "$TMP" "$OUTPUT_SYMLINK"
        then
            echo "Existing file is already up to date."
            rm -f "$TMP"
            continue
        fi
        rm "$OUTPUT_SYMLINK"
    fi

    mv "$TMP" "$OUTPUT_FILE"
    ln -s "$OUTPUT_FILE_NAME" "$OUTPUT_SYMLINK"
done