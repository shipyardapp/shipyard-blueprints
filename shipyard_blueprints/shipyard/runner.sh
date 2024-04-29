set -o allexport 
source .env set +o allexport


if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_api/cli/get_logs.py \
        "--api-key" $SHIPYARD_API_TOKEN \
        "--organization-id" $ORG_ID \
        "--file-name" "test_logs.csv"
fi




