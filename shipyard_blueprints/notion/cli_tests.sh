#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport 

if [ "$1" = "upr" ]; then
    echo "Starting replace job with Database ID"
    python3 shipyard_notion/cli/upload.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --source-file-name sample.csv --insert-method replace
fi

if [ "$1" = 'upa' ]; then
    echo "Starting append job"
    python3 shipyard_notion/cli/upload.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --source-file-name sample.csv --insert-method append
fi


if [ "$1" = "dlc" ]; then
    echo "Starting download job as csv"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --destination-file-name results.csv --file-type csv

fi

if [ "$1" = "dlj" ]; then
    echo "Starting download job as json"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --destination-file-name results.json --file-type json

fi

# test replacement without database id
if [ "$1" = 'uprn' ]; then
    echo "Starting replace job without database id"
    python3 shipyard_notion/cli/upload.py --token $NOTION_ACCESS_TOKEN --source-file-name sample.csv --insert-method replace
fi

# fetch data with notion specific datatypes
if [ "$1" = 'dlj2' ]; then
    echo "Starting download of nonstandard datatypes to JSON"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --destination-file-name nonstandard.json --database-id $DB2 --file-type json
fi


if [ "$1" = "dlc2" ]; then
    echo "Starting download of nonstandard datatypes to CSV"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --database-id $DB2 --destination-file-name nonstandard.csv --file-type csv

fi
