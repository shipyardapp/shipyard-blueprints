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

if [ "$1" = 'rf1' ]; then 
    echo "Beginning Refresh"
    python3 ./shipyard_domo/cli/refresh_dataset.py \
        --client-id $DOMO_CLIENT_ID \
        --secret-key $DOMO_SECRET_KEY \
        --dataset-id $D_REFRESH \
        --wait-for-completion "TRUE"

fi

if [ "$1" = 'ec1' ]; then 
    echo "Beginning to download card"
    python3 ./shipyard_domo/cli/export_card_to_file.py \
       --domo-instance $DOMO_INSTANCE \
       --card-id $DOMO_CARD_ID \
       --destination-file-name "card.csv" \
       --file-type "csv" \
       --developer-token $DOMO_ACCESS_TOKEN
fi
        







