#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport

sa="$GOOGLE_APPLICATION_CREDENTIALS"

if [ "$1" = "upload" ]; then
    echo "Starting upload"
    python3 ./shipyard_googledrive/cli/upload.py --service-account $GOOGLE_APPLICATION_CREDENTIALS --source-file-name sample.csv  --source-file-name-match-type exact_match
fi


if [ "$1" = "download" ]; then
    echo "Starting download with no source folder and no source file"
    python3 ./shipyard_googledrive/cli/download.py --service-account "$sa" --source-file-name-match-type "exact_match" --source-file-name "newfile.csv" --destination-folder-name "results2" --drive "Blueprint Shared Drive"
    sleep 1
    # multiple file download
    echo "Starting download with destination folder, no source folder, regex match, and no destination rename"
    python3 ./shipyard_googledrive/cli/download.py --service-account "$sa" --source-file-name-match-type "regex_match" --source-file-name "csv" --destination-folder-name "regex_download" --drive "Blueprint Shared Drive"
    sleep 1
    echo "Starting download with destination folder, no source folder, regex match, and destination rename"
    python3 ./shipyard_googledrive/cli/download.py --service-account "$sa" --source-file-name-match-type "regex_match" --source-file-name "csv" --destination-folder-name "reg2" --drive "Blueprint Shared Drive" --destination-file-name "gdata.csv"
fi
