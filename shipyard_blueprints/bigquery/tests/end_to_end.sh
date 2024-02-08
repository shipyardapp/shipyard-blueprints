set -o allexport 
source .env set +o allexport

TABLE="end_to_end"
TABLE_MULTIPLE="multiple_upload"
TABLE_DTS="end_to_end_dts" # data types



if [ "$1" = 'up1' ]; then 
    echo "Begininng upload of a single file to replace a table"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name $SINGLE_FILE 
fi

if [ "$1" = 'up2' ]; then 
    echo "Begininng upload of single file within folder to replace"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name "data_1.csv" \
        --source-folder-name $FOLDER
    
fi

if [ "$1" = 'up3' ]; then 
    echo "Begininng upload of single file within folder to append"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "append" \
        --source-file-name "data_1.csv" \
        --source-folder-name $FOLDER
fi


