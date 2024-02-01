set -o allexport 
source .env set +o allexport


# upload single file to replace table


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
    --table-name "end_to_end"

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
    --table-name "end_to_end"
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
    --table-name "end_to_end"
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
    --table-name "end_to_end"
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
    --table-name "end_to_end_dts" \
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
    --table-name "end_to_end_dts" \
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
    --table-name "end_to_end_dts" \
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
    --table-name "end_to_end_dts" \
    --snowflake-data-types '[["string_col", "string"], ["char_col", "char"], ["int_col", "INT"], ["float_col", "float"], ["bool_col","boolean"], ["date_col", "date"], ["datetime_col", "timestamp"]]'
fi

#TODO: test to ensure that if append is selected and the table doesn't exist, it gets created and inserted properly


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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
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
    --table-name "multiple_upload" \
    --source-file-name-match-type "regex_match" \
    --snowflake-data-types '{"string_col": "String", "char_col": "Char", "int_col": "Int", "float_col": "Number", "bool_col": "Boolean", "date_col": "Date", "datetime_col": "Timestamp"}'
fi

# execute query section
if [ "$1" = 'ex1' ]; then 
    echo "Dropping table end_to_end_dts"
    python3 ./shipyard_snowflake/cli/execute_sql.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --query "drop table end_to_end_dts;"
fi
