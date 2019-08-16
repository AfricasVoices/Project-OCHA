#!/bin/bash

set -e

IMAGE_NAME=export-reach-contact-list

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: ./docker-run.sh
    <traced-data-path> <phone-number-uuid-table-path> <csv-output-path>"
    exit
fi

# Assign the program arguments to bash variables.
INPUT_TRACED_DATA=$1
INPUT_PHONE_NUMBER_UUID_TABLE=$2
OUTPUT_CSV=$3

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u export_reach_contact_list.py \
    /data/traced-data.json /data/phone-number-uuid-table.json /data/output.csv
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_TRACED_DATA" "$container:/data/traced-data.json"
docker cp "$INPUT_PHONE_NUMBER_UUID_TABLE" "$container:/data/phone-number-uuid-table.json"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_CSV")"
docker cp "$container:/data/output.csv" "$OUTPUT_CSV"

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
