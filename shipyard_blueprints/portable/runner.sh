set -o allexport
source .env set +o allexport

if [ "$1" = "tr1" ]; then 
    echo "Starting to trigger job"
    python3 ./shipyard_portable/cli/trigger.py \
        --access-token $PORTABLE_API_TOKEN \
        --flow-id $PORTABLE_FLOW_1 \
        --wait-for-completion 'TRUE'
fi




