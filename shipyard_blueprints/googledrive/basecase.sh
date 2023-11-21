#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport

if [ "$1" = "upload" ]; then
    echo "Starting upload"
    python3 ./shipyard_googledrive/cli/upload.py --service-account $GOOGLE_APPLICATION_CREDENTIALS --source-file-name sample.csv  --source-file-name-match-type exact_match
fi
