#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport
if [ "$1" = 'up1' ]; then
    echo "Starting upload of smartsheet with replace (creating a new sheet)"
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name sample.csv --sheet-name testload --insert-method replace
fi

if [ "$1" = 'up2' ]; then
    echo "Starting upload of sheet to replace existing sheet"
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name small.csv --sheet-id $SMALL --insert-method replace

fi

if [ "$1" = 'up3' ]; then 
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name small.csv --sheet-name small
fi

if [ "$1" = 'upa1' ]; then 
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --source-file-name small.csv --sheet-name small --insert-method append --sheet-id $SMALL
fi


if [ "$1" = 'down1' ]; then 
    python3 ./shipyard_smartsheets/cli/download.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id $SMALL --destination-file-name download.csv
fi

if [ "$1" = 'down2' ]; then 
    python3 ./shipyard_smartsheets/cli/download.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id $SMALL --destination-file-name test_download.xlsx --file-type xlsx
fi


if [ "$1" = 'failing' ]; then
    echo "starting failing fleet"
    python3 ./shipyard_smartsheets/cli/download.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id 7HVj9XfXqgRqfJvXX9PP3F8H3pRHrhCm5W35Hpq1 --destination-file-name sheet.csv --file-type csv
    echo "Download complete, beginning upload now"
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-name "shipyard load" --source-file-name sheet.csv --insert-method replace --file-type csv
fi

if [ "$1" = 'invalid_id' ]; then 
    echo "Starting append test that should fail with an invalid sheet id"
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id "1243lkasdfl" --source-file-name sheet.csv --insert-method append 
    echo "Starting replace test that should fail with an invalid sheet id"
    python3 ./shipyard_smartsheets/cli/upload.py --access-token $SMARTSHEET_ACCESS_TOKEN --sheet-id "1243lkasdfl" --source-file-name sheet.csv --insert-method replace 

fi
