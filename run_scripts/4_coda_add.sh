#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="OCHA"
DATASETS=(
    "gender"
    "age"
    "recently_displaced"
    "in_idp_camp"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout '6e4b5e280b0baded86bc2afbf27f44f542e1aeed'  # (master before we started work on segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done

DATASETS=(
    "s04e01"
    "s04e02"
    "location"
    "have_voice"
    "suggestions"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout '6019a60c855f5da3d82ad8d1423303ce4d6914b6'  # (master which supports segmenting)

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
