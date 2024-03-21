set -o allexport 
source .env set +o allexport


if [ "$1" = 'up1' ]; then
    python3 ./shipyard_redshift/cli/upload.py \
        --username "$REDSHIFT_USERNAME" \
        --password "$REDSHIFT_PASSWORD" \
        --host "$REDSHIFT_HOST" \
        --database "$REDSHIFT_DATABASE" \
        --port "$REDSHIFT_PORT" \
        --source-file-name-match-type "exact_match" \
        --source-file-name "soccer.csv" \
        --table-name $UP_TABLE \
        --insert-method "replace"


fi



