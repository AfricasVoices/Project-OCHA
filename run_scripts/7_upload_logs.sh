#!/usr/bin/env bash

set -e

if [[ $# -ne 6 ]]; then
    echo "Usage: ./7_upload_logs <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <run-id> <memory-profile-file-path> <data-archive-file-path>"
    echo "Uploads the pipeline logs"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
PIPELINE_CONFIGURATION_FILE_PATH=$3
RUN_ID=$4
MEMORY_PROFILE_FILE_PATH=$5
DATA_ARCHIVE_FILE_PATH=$6

cd ..
./docker-run-upload-logs.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$PIPELINE_CONFIGURATION_FILE_PATH" \
    "$RUN_ID" "$MEMORY_PROFILE_FILE_PATH" "$DATA_ARCHIVE_FILE_PATH"
