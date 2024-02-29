set -o allexport 
source .env set +o allexport


if [ "$1" = 'up1' ]; then 
    echo "Beginning upload"
    python3 ./shipyard_domo/cli/upload.py \
        --client-id $DOMO_CLIENT_ID \
        --secret-key $DOMO_SECRET_KEY \
        --file-name "soccer.csv" \
        --dataset-id $DS_DL_BASE_CASE \
        --insert-method 'REPLACE' \
        --dataset-name "Pytest Base Case" \
        --dataset-description "This is a base case for backwards compatibility" \
        --source-file-match-type "exact_match"

fi

