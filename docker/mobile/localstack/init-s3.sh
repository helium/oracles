#!/usr/bin/env bash
echo "Init localstack s3"
awslocal s3 mb s3://mobile-ingest
awslocal s3 mb s3://mobile-verifier
awslocal s3 mb s3://mobile-packet-verifier
awslocal s3 mb s3://mobile-price
awslocal s3 mb s3://mobile-verifier-data-sets

# This shell script automates the process of uploading files from local directories
# to S3 buckets using `awslocal` (typically used with LocalStack for local AWS service emulation).
#
# 1. Define Source Directories: The script begins by setting the `dirs` variable to include
#    all directories under `/tmp/data/`.
#
# 2. Directory Iteration: It loops through each directory found in `/tmp/data/*`,
#    processing one directory at a time.
#
# 3. Extract Bucket Name: For each directory, the script extracts the directory name
#    (using `basename`) and assigns it as the S3 bucket name.
#
# 4. File Iteration: Within each directory, the script iterates over all files,
#    checking if each item is a file (excluding subdirectories).
#
# 5. Upload to S3: For each file, the script uploads it to the corresponding S3 bucket
#    using the `awslocal s3 cp` command. The file is placed in the S3 bucket with its original filename.
#
# 6. Debug Output: After each upload, the script prints the executed command for verification
#    and debugging purposes, followed by a separator line for readability.
dirs=/tmp/data/*
for dir in $dirs; do
    echo "Looking @ $dir"
    bucket=$(basename "$dir")

    for file in "$dir"/*; do
        if [[ -f "$file" ]]; then
            echo "Uploading $file to bucket $bucket"
            file_name=$(basename "$file")

            # Perform the upload
            awslocal s3 cp "$file" "s3://$bucket/$file_name"

            # Debugging output to confirm upload command
            echo "Executed: awslocal s3 cp \"$file\" \"s3://$bucket/$file_name\""
            echo "################################################################"
        fi
    done
done
