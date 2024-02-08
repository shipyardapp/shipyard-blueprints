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

if [ "$1" = 'up4' ]; then 
    echo "Begininng upload of single file to append"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "append" \
        --source-file-name $SINGLE_FILE
fi


# begin testing data types
if [ "$1" = 'up5' ]; then 
    echo "Beginning upload of a single file with datatypes"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name $SINGLE_FILE \
        --schema '[["string_col", "string"], ["char_col", "string"], ["int_col", "INT64"], ["float_col", "FLOAT64"], ["bool_col","bool"], ["date_col", "date"], ["datetime_col", "datetime"]]'
fi


if [ "$1" = 'ex1' ]; then 
    echo "Dropping table $TABLE"
    python3 ./shipyard_bigquery/cli/execute_query.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "DROP TABLE $DATASET.$TABLE"
fi




