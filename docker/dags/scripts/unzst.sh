#!/bin/bash

# Usage: ./process_file.sh input_path output_folder chunk_size output_base

INPUT_PATH=$1
OUTPUT_FOLDER=$2
CHUNK_SIZE=$3
OUTPUT_BASE=$4

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_FOLDER"

# Decompress and split the file
zstdcat "$INPUT_PATH" | split -b "$CHUNK_SIZE" - "$OUTPUT_BASE"_part_.json

if [ $? -eq 0 ]; then
    echo "File successfully decompressed and split."
else
    echo "An error occurred during decompression or splitting."
    exit 1
fi
