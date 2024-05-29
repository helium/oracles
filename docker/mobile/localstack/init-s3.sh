#!/usr/bin/env bash
echo "Init localstack s3"
awslocal s3 mb s3://mobile-ingest
awslocal s3 mb s3://mobile-verifier
awslocal s3 mb s3://mobile-packet-verifier
awslocal s3 mb s3://mobile-price
awslocal s3 mb s3://mobile-verifier-data-sets

# Description:
#   This Bash script uploads files from subdirectories in /tmp/data to an S3-compatible service using awslocal.
#   It treats each subdirectory as a separate bucket and uploads files with modified timestamps in their filenames.
#
# Script Overview:
#   1. Iterate Through Directories: The script iterates over each subdirectory in /tmp/data.
#   2. Extract Bucket Name: The script extracts the bucket name from each subdirectory's basename.
#   3. Process Files in Each Directory: For each file in the subdirectory, the script:
#      - Prints the original file name and the bucket name for debugging purposes.
#      - Generates a new file name by replacing any existing 13-digit timestamp with the current timestamp in milliseconds.
#      - Prints the new file name and the S3 path for debugging purposes.
#      - Uploads the file to the specified S3 bucket using awslocal s3 cp.
#
# Key Points:
#   - Directories and Buckets: Each subdirectory in /tmp/data is treated as a bucket.
#   - File Processing: Files within these directories are uploaded to their respective buckets with a new timestamp in their filenames.

dirs=/tmp/data/*
for dir in $dirs; do
    echo "Looking @ $dir"
    readarray -d '/' -t array <<<"$dir"
    bucket=$(echo "${array[-1]}" | tr -d '\n')

    for file in "$dir"/*; do
        if [[ -f "$file" ]]; then
            echo "Uploading $file to bucket $bucket"
            now=$(date +%s000)
            file_name=$(basename "$file")

            # Debugging output to check the file name and bucket
            echo "Original file name: $file_name"
            echo "Current timestamp: $now"

            # Replace timestamp in file name
            new_file=$(echo "$file_name" | sed -E 's|[0-9]{13}|'"${now}"'|g')

            # Debugging output to check the new file name and bucket path
            echo "New file name: $new_file"
            echo "s3 path: s3://$bucket/$new_file"

            # Perform the upload
            awslocal s3 cp "$file" "s3://$bucket/$new_file"

            # Debugging output to confirm upload command
            echo "Executed: awslocal s3 cp \"$file\" \"s3://$bucket/$new_file\""
            echo "################################################################"
        fi
    done
done
