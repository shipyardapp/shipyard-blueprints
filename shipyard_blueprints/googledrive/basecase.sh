#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport

sa="$GOOGLE_APPLICATION_CREDENTIALS"

if [ "$1" = "upload" ]; then
    echo "Starting upload with no destination folder, no rename and exact match"
    python3 ./shipyard_googledrive/cli/upload.py --service-account "$sa" --source-file-name sample.csv  --source-file-name-match-type exact_match --drive "0ADIj7rHQKNmWUk9PVA"

    echo "Starting upload with destination folder and regex match"
    python3 ./shipyard_googledrive/cli/upload.py  --service-account "$sa" --source-folder-name reg2 --source-file-name-match-type regex_match --source-file-name csv --drive "0ADIj7rHQKNmWUk9PVA"

    echo "Starting upload with local folder, exact match, drive folder, and rename"
    python3 ./shipyard_googledrive/cli/upload.py --service-account "$sa" --source-file-name-match-type 'exact_match' --source-folder-name reg2 --source-file-name gdata_1.csv --destination-folder-name "1OiKjcPUomfGyIt-sco_5cryAJoh16yzr" --destination-file-name comptability_file.csv --drive "0ADIj7rHQKNmWUk9PVA"
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
    sleep 1

    echo "Starting download with source folder, regex match, destination folder name, and destination rename"
    python3 ./shipyard_googledrive/cli/download.py --service-account "$sa" --source-file-name-match-type 'regex_match' --source-file-name "csv" --destination-folder-name "reg3" --drive "Blueprint Shared Drive" --destination-file-name "reg_gdata.csv"

    sleep 1
    echo "Starting download of single file with source folder and destination rename"
    python3 ./shipyard_googledrive/cli/download.py  --service-account "$sa" --source-file-name-match-type 'exact_match' --source-folder-name test_hidden --source-file-name hidden.csv --destination-folder-name gdrive_hidden --destination-file-name newhidden.csv

fi
