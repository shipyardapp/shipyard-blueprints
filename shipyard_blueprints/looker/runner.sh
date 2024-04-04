set -o allexport 
source .env set +o allexport

if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_looker/cli/download_dashboard.py \
        "--base-url" $LOOKER_URL \
        "--client-id" $LOOKER_CLIENT_ID \
        "--client-secret" $LOOKER_CLIENT_SECRET \
        "--dashboard-id" 14 \
        "--output-width" 800 \
        "--output-height" 600 \
        "--destination-file-name" "test.png" \
        "--file-type" "png"
fi
