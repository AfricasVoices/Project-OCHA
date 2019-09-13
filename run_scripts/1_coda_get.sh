#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="OCHA"
DATASETS=(
    "s04e01"
    "s04e02"

    "location"
    "gender"
    "age"
    "recently_displaced"
    "in_idp_camp"

    "have_voice"
    "suggestions"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout '6019a60c855f5da3d82ad8d1423303ce4d6914b6'  # (master which supports segmenting)

mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    echo "Getting messages data from ${PROJECT_NAME}_${DATASET}..."

    pipenv run python get.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages >"$DATA_ROOT/Coded Coda Files/$DATASET.json"
done
