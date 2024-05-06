set -o allexport 
source .env set +o allexport


if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_api/cli/get_logs.py \
        "--api-key" $SHIPYARD_API_TOKEN \
        "--organization-id" $ORG_ID \
        "--file-name" "test_logs.csv"
fi

if [ "$1" = 'tr1' ]; then 
    python3 ./shipyard_api/cli/trigger_fleet.py \
        "--api-key" $SHIPYARD_API_TOKEN \
        "--organization-id" $ORG_ID \
        "--fleet-id" $PROD_FLEET_ID \
        "--project-id" $PROD_PROJECT


fi




