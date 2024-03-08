set -o allexport 
source .env set +o allexport

TABLE="end_to_end"
TABLE_MULTIPLE="multiple_upload"
TABLE_DTS="end_to_end_dts"

# Upload section
if [ "$1" =  'up1' ]; then 
    echo "Starting upload of single file to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "replace" \
    --table-name $TABLE

fi

if [ "$1" = 'up2' ]; then 
    echo "Starting upload of single file to append table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "append" \
    --table-name "$TABLE"
fi

 
if [ "$1" = 'up3' ]; then
    echo "Starting upload of a single file with a folder location to replace a table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $NESTED_FILE \
    --source-folder-name "test_folder" \
    --insert-method "replace" \
    --table-name "$TABLE"
fi

 
if [ "$1" = 'up4' ]; then
    echo "Starting upload of a single file with a folder location to append to a table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $NESTED_FILE \
    --source-folder-name "test_folder" \
    --insert-method "append" \
    --table-name "$TABLE"
fi


if [ "$1" = 'up5' ]; then
    echo "Starting upload of a single file with datatypes to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "replace" \
    --table-name "$TABLE_DTS" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi



if [ "$1" = 'up6' ]; then
    echo "Starting upload of a single file with datatypes, the legacy method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "replace" \
    --table-name "$TABLE_DTS" \
    --snowflake-data-types '[["string_col", "string"], ["char_col", "char"], ["int_col", "INT"], ["float_col", "float"], ["bool_col","boolean"], ["date_col", "date"], ["datetime_col", "timestamp"]]'
fi

if [ "$1" = 'up7' ]; then
    echo "Starting upload of a single file with datatypes to append table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "append" \
    --table-name "$TABLE_DTS" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi

if [ "$1" = 'up8' ]; then
    echo "Starting upload of a single file with datatypes to append table, using legacy method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE \
    --insert-method "append" \
    --table-name "$TABLE_DTS" \
    --snowflake-data-types '[["string_col", "string"], ["char_col", "char"], ["int_col", "INT"], ["float_col", "float"], ["bool_col","boolean"], ["date_col", "date"], ["datetime_col", "timestamp"]]'
fi


if [ "$1" = 'up9' ]; then
    echo "Starting upload of multiple files to replace a table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --source-folder-name "test_folder" \
    --insert-method "replace" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match"
fi


if [ "$1" = 'up10' ]; then
    echo "Starting upload of multiple files to append to a table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --source-folder-name "test_folder" \
    --insert-method "append" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match"
fi

if [ "$1" = 'up11' ]; then
    echo "Starting upload of multiple files to replace a table, with no folder"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "replace" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match"
fi

if [ "$1" = 'up12' ]; then
    echo "Starting upload of multiple files with datatypes to replace table, legacy method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "replace" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '[["string_col", "string"], ["char_col", "char"], ["int_col", "INT"], ["float_col", "float"], ["bool_col","boolean"], ["date_col", "date"], ["datetime_col", "timestamp"]]'
fi

if [ "$1" = 'up13' ]; then
    echo "Starting upload of multiple files with datatypes to append table, legacy method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "append" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '[["string_col", "string"], ["char_col", "char"], ["int_col", "INT"], ["float_col", "float"], ["bool_col","boolean"], ["date_col", "date"], ["datetime_col", "timestamp"]]'
fi


if [ "$1" = 'up14' ]; then
    echo "Starting upload of multiple files with datatypes to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "replace" \
    --table-name "$TABLE_MULTIPLE" \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi


if [ "$1" = 'up15' ]; then
    echo "Starting upload of multiple files with datatypes to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "append" \
    --table-name $TABLE_MULTIPLE \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi


if [ "$1" = 'up16' ]; then
    echo "Starting upload of multiple files using the add insert method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "mult"\
    --insert-method "add" \
    --table-name $TABLE_MULTIPLE \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi

if [ "$1" = 'up17' ]; then
    echo "Starting upload of a single file using the add insert method"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name $LOCAL_FILE\
    --insert-method "add" \
    --table-name $TABLE \
    --source-file-name-match-type "exact_match" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi



# execute query section
if [ "$1" = 'ex1' ]; then 
    echo "Dropping table $TABLE"
    python3 ./shipyard_snowflake/cli/execute_sql.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "drop table $TABLE"
fi

if [ "$1" = 'ex2' ]; then 
    echo "Dropping table $TABLE_MULTIPLE"
    python3 ./shipyard_snowflake/cli/execute_sql.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "drop table $TABLE_MULTIPLE"
fi

## intentionally bad query 
if [ "$1" = 'ex3' ]; then 
    echo "This is a bad query and should fail"
    python3 ./shipyard_snowflake/cli/execute_sql.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "slllect * from bad table"
fi

if [ "$1" = 'fe1' ]; then 
    echo "Fetching query results"
    python3 ./shipyard_snowflake/cli/fetch.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "SELECT * FROM RAW_SOCCERRANKINGS" \
    --destination-file-name "output.csv"

fi

if [ "$1" = 'fe2' ]; then 
    echo "Fetching query results"
    python3 ./shipyard_snowflake/cli/fetch.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "SELECT * FROM RAW_SOCCERRANKINGS" \
    --destination-file-name "output.csv" \
    --destination-folder-name "new_folder"
fi


if [ "$1" = 'fe3' ]; then 
    echo "Fetching query results"
    python3 ./shipyard_snowflake/cli/fetch.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "SELECT * FROM RAW_SOCCERRANKINGS" \
    --destination-file-name "output.csv" \
    --destination-folder-name "test_folder"
fi




