#!/bin/bash

set -e

IMAGE_NAME=ocha

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 6 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>]
    <user> <google-cloud-credentials-file-path> <pipeline-configuration-file-path> <messages-traced-data> <individuals-traced-data> <output-dir>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
INPUT_GOOGLE_CLOUD_CREDENTIALS=$2
INPUT_PIPELINE_CONFIGURATION=$3
INPUT_MESSAGES_TRACED_DATA=$4
INPUT_INDIVIDUALS_TRACED_DATA=$5
OUTPUT_DIR=$6

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run $PROFILE_CPU_CMD python -u generate_analysis_graphs.py \
    \"$USER\" /credentials/google-cloud-credentials.json /data/pipeline_configuration.json \
    /data/messages-traced-data.jsonl /data/individuals-traced-data.jsonl /data/output-graphs
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_PIPELINE_CONFIGURATION" "$container:/data/pipeline_configuration.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_MESSAGES_TRACED_DATA" "$container:/data/messages-traced-data.jsonl"
docker cp "$INPUT_INDIVIDUALS_TRACED_DATA" "$container:/data/individuals-traced-data.jsonl"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$OUTPUT_DIR"
docker cp "$container:/data/output-graphs/." "$OUTPUT_DIR"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
