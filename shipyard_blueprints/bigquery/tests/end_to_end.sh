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
        --schema '[["string_col", "string"], ["char_col", "string"], ["int_col", "Integer"], ["float_col", "Float"], ["bool_col","bool"], ["date_col", "date"], ["datetime_col", "datetime"]]'
        # --schema '[["string_col", "string"], ["char_col", "string"], ["int_col", "INT64"], ["float_col", "FLOAT64"], ["bool_col","bool"], ["date_col", "date"], ["datetime_col", "datetime"]]'
fi

if [ "$1" = 'up6' ]; then 
    echo "Beginning upload of a single file with datatypes"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name $SINGLE_FILE \
        --schema '[{"name": "string_col", "type": "string"}, {"name": "char_col", "type": "string"}, {"name": "int_col", "type": "INTEGER"}, {"name": "float_col", "type": "Float64"}, {"name": "bool_col", "type": "Bool"}, {"name": "date_col", "type": "Date"},{"name": "datetime_col", "type": "Datetime"}]'
        # --schema '[{"name": "string_col", "type": "string"}, {"name": "char_col", "type": "string"}, {"name": "int_col", "type": "Int64"}, {"name": "float_col", "type": "Float64"}, {"name": "bool_col", "type": "Bool"}, {"name": "date_col", "type": "Date"},{"name": "datetime_col", "type": "Datetime"}]'
        # --schema '{"string_col": "String", "char_col": "string", "int_col": "Int64", "float_col": "Float64", "bool_col": "Bool", "date_col": "Date", "datetime_col": "Datetime"}'

fi

if [ "$1" = 'up7' ]; then 
    echo "Beginning upload of multiple files to repalce a dataset"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE_MULTIPLE \
        --upload-type "overwrite" \
        --source-file-name-match-type "regex_match" \
        --source-file-name "csv" 
fi

if [ "$1" = 'up8' ]; then 
    echo "Beginning upload of multiple files to append a dataset"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE_MULTIPLE \
        --upload-type "append" \
        --source-file-name-match-type "regex_match" \
        --source-file-name "csv" \
        --source-folder-name "test_folder"
fi

if [ "$1" = 'up9' ]; then 
    echo "Beginning upload of a single file with datatypes with an incorrect schema"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name $SINGLE_FILE \
        --schema '[{"name": "string_col", "type": "string"}, {"name": "char_col", "type": "char"}, {"name": "int_col", "type": "Int64"}, {"name": "float_col", "type": "Float64"}, {"name": "bool_col", "type": "Bool"}, {"name": "date_col", "type": "Date"},{"name": "datetime_col", "type": "Datetime"}]'
        # --schema '{"string_col": "String", "char_col": "string", "int_col": "Int64", "float_col": "Float64", "bool_col": "Bool", "date_col": "Date", "datetime_col": "Datetime"}'

fi




# execute query tests
if [ "$1" = 'ex1' ]; then 
    echo "Dropping table $TABLE"
    python3 ./shipyard_bigquery/cli/execute_query.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "DROP TABLE $DATASET.$TABLE"
fi

if [ "$1" = 'ex2' ]; then 
    echo "Dropping table $TABLE_MULTIPLE"
    python3 ./shipyard_bigquery/cli/execute_query.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "DROP TABLE $DATASET.$TABLE_MULTIPLE"
fi

if [ "$1" = 'ex3' ]; then 
    echo "Bad Query example"
    python3 ./shipyard_bigquery/cli/execute_query.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "SSEEELLLECTTT 1"
fi

# download locally tests
if [ "$1" = 'fe1' ]; then 
    echo "Downloading query results to folder" 
    python3 ./shipyard_bigquery/cli/download.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "$QUERY" \
        --destination-folder-name  $FOLDER \
        --destination-file-name "output.csv"
fi


if [ "$1" = 'fe2' ]; then 
    echo "Downloading query results to a new folder" 
    python3 ./shipyard_bigquery/cli/download.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --query "$QUERY" \
        --destination-folder-name "new_folder" \
        --destination-file-name "output.csv"
fi


## incorrect datatype test

if [ "$1" = 'up-bad1' ]; then 
    echo "Beginning upload of a single file with datatypes"
    python3 ./shipyard_bigquery/cli/upload.py --service-account "$GOOGLE_APPLICATION_CREDENTIALS" \
        --dataset $DATASET \
        --table $TABLE \
        --upload-type "overwrite" \
        --source-file-name $SINGLE_FILE \
        --schema '[{"name": "string_col", "type": "string"}, {"name": "char_col", "type": "char"}, {"name": "int_col", "type": "INTEGER"}, {"name": "float_col", "type": "Float"}, {"name": "bool_col", "type": "Boolean"}, {"name": "date_col", "type": "Date"},{"name": "datetime_col", "type": "Datetime"}]'
fi

