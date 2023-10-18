#!/bin/bash
# use the enviornment variables from the .env file
set -o allexport 
source .env set +o allexport 

# if [ "$1" -eq 1 ]; then
#     echo "Starting Base Case test\n"
#     python3 ./shipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv
#
# elif [ "$1" -eq 2 ]; then
#     echo "Starting Append test\n"
#     python3 ./shipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv --url "https://www.notion.so/shipyardapp/f2e4fadef23a41babb82a4360d163b41"
#
# elif [ "$1" -eq 3 ]; then
#     echo "Starting replace test\n"
#     python3 .sshipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv --url "https://www.notion.so/shipyardapp/f2e4fadef23a41babb82a4360d163b41" --insert-method replace
# fi


if [ "$1" = "upr" ]; then
    echo "Starting replace job"
    python3 shipyard_notion/cli/upload.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --source-file-name sample.csv --insert-method replace
fi


if [ "$1" = "dlc" ]; then
    echo "Starting download job as csv"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --destination-file-name results.csv --file-type csv

fi

if [ "$1" = "dlj" ]; then
    echo "Starting download job as json"
    python3 shipyard_notion/cli/download.py --token $NOTION_ACCESS_TOKEN --database-id $NOTION_DB --destination-file-name results.json --file-type json

fi







