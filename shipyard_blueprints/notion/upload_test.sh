if [ "$1" -eq 1 ]; then
    echo "Starting Base Case test\n"
    python3 ./shipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv

elif [ "$1" -eq 2 ]; then
    echo "Starting Append test\n"
    python3 ./shipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv --url "https://www.notion.so/shipyardapp/f2e4fadef23a41babb82a4360d163b41"

elif [ "$1" -eq 3 ]; then
    echo "Starting replace test\n"
    python3 .sshipyard_notion/cli/upload.py --token-v2 $NOTION_TOKEN_V2 --file-name sample.csv --url "https://www.notion.so/shipyardapp/f2e4fadef23a41babb82a4360d163b41" --insert-method replace
fi







