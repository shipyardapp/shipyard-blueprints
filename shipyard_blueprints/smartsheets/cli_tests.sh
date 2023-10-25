#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport
if [ "$1" = 'up1' ]; then
    echo "Starting upload of smartsheet"
    python3 ./shipayrd_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name sample.csv --sheet-name testload
fi

if [ "$1" = 'up2' ]; then 
    python3 ./shipayrd_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name small.csv --sheet-name small
fi

if [ "$1" = 'down1' ]; then 
    python3 ./shipayrd_smartsheets/cli/download.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id $SMALL --destination-file-name download.csv
fi
