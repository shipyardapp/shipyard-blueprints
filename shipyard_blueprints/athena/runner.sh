set -o allexport 
source .env set +o allexport

if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_athena/cli/download_query.py \
        --aws-access-key-id $AWS_ACCESS_KEY_ID \
        --aws-secret-access-key $AWS_SECRET_ACCESS_KEY \
        --aws-default-region $REGION \
        --bucket-name $BUCKET_NAME \
        --destination-file-name "output.csv" \
        --database "shipyardtest" \
        --query "select * from persons where id is not null limit 100;"
fi
