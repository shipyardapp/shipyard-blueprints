set -o allexport 
source .env set +o allexport

# upload the file to databricks 

server_host="$DATABRICKS_SERVER_HOST"
http_path="$DATABRICKS_HTTP_PATH"
token="$DATABRICKS_SQL_ACCESS_TOKEN"
#
# echo "Uploading databricks.csv to Databricks"
# python3 ./shipyard_databricks_sql/cli/upload.py --access-token $DATABRICKS_SQL_ACCESS_TOKEN \
#     --server-host $DATABRICKS_SERVER_HOST \
#     --http-path $DATABRICKS_HTTP_PATH \
#     --catalog $CATALOG2 \
#     --schema $SCHEMA2 \
#     --table-name "shipyard_test" \
#     --insert-method "replace" \
#     --file-type "csv" \
#     --file-name "databricks.csv"


if [ "$1" = 'up1' ]; then
    echo "Testing regex upload for csvs"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --table-name "reg_test" \
    --insert-method "replace" \
    --file-type "csv" \
    --file-name "csv" \
    --match-type "glob_match"
fi

if [ "$1" = 'up2' ]; then
    echo "Starting single parquet file upload"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --table-name "parquet_test" \
    --insert-method "replace" \
    --file-type "parquet" \
    --file-name "test.parquet" \
    --match-type "exact_match"
fi

if [ "$1" = 'up3' ]; then
    echo "Starting regex match for parquet files"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $DEMO_CATALOG \
    --schema $DEMO_SCHEMA \
    --table-name "glob_test" \
    --insert-method "replace" \
    --file-type "parquet" \
    --file-name "*.parquet" \
    --match-type "glob_match" \
    --folder-name "mult"
    
fi

if [ "$1" = 'up4' ]; then
    echo "Starting data type test for parquet file"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --table-name "parquet_test" \
    --insert-method "replace" \
    --file-type "parquet" \
    --file-name "test.parquet" \
    --match-type "exact_match" \
    --data-types '{"string_col": "string", "char_col": "string", "int_col": "int", "float_col": "double", "bool_col": "boolean", "date_col": "date", "datetime_col": "timestamp"}'
fi

if [ "$1" = 'up5' ]; then 
    echo "Starting upload for creating a new table"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --table-name "staging_failing" \
    --insert-method "replace" \
    --file-type "csv" \
    --file-name "databricks.csv" \
    --match-type "exact_match" 

fi

# download section
if [ "$1" = 'dl1' ]; then 
    echo "Starting download of parquet"
    python3 ./shipyard_databricks_sql/cli/fetch.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --file-name  "testdownload.parquet" \
    --query "select * from parquet_test" \
    --folder-name "parq_download"  
fi

if [ "$1" = 'dl2' ]; then 
    echo "Starting download of parquet"
    python3 ./shipyard_databricks_sql/cli/fetch.py --access-token $token \
    --server-host $server_host \
    --http-path $http_path \
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --file-name  "testdownload.csv" \
    --query "select * from parquet_test" \
    --folder-name "csv_download"  

fi

if [ "$1" = 'demo-up' ]; then
    echo "Starting upload for demo"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $DATABRICKS_SERVER_HOST \
    --http-path $DATABRICKS_HTTP_PATH \
    --catalog $DEMO_CATALOG \
    --schema $DEMO_SCHEMA \
    --volume $DEMO_VOLUME \
    --table-name $DEMO_TABLE \
    --insert-method "append" \
    --file-type "parquet" \
    --file-name $DEMO_FILE \
    --match-type "exact_match" 

fi

if [ "$1" = 'demo-up2' ]; then 
        echo "Starting upload for demo"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $token \
    --server-host $DATABRICKS_SERVER_HOST \
    --http-path $DATABRICKS_HTTP_PATH \
    --catalog $DEMO_CATALOG \
    --schema $DEMO_SCHEMA \
    --volume $DEMO_VOLUME \
    --table-name $DEMO_CSV_TABLE \
    --insert-method "append" \
    --file-type "csv" \
    --file-name $DEMO_CSV_FILE \
    --match-type "exact_match" 
fi



if [ "$1" = 'up-glob' ]; then 
        echo "Starting glob upload, with replace"
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $DATABRICKS_SQL_ACCESS_TOKEN \
    --server-host $DATABRICKS_SERVER_HOST \
    --http-path $DATABRICKS_HTTP_PATH \
    --catalog $DEMO_CATALOG \
    --schema $DEMO_SCHEMA \
    --volume $DEMO_VOLUME \
    --table-name "parq_glob" \
    --insert-method "replace" \
    --file-type "parquet" \
    --file-name "*.parquet" \
    --match-type "glob_match"  \
    --folder-name "mult"
fi


if [ "$1" = 'down-test' ]; then 
    echo "Starting download of parquet"
    python3 ./shipyard_databricks_sql/cli/fetch.py --access-token $token \
    --server-host $DATABRICKS_SERVER_HOST \
    --http-path $DATABRICKS_HTTP_PATH \
    --catalog "shipyard_demos" \
    --schema "domo_extracts" \
    --file-name  "testdownload.parquet" \
    --query "select * from domo_soccer" \
    --folder-name "parq_download"  \
    --file-type "parquet"
    
fi


if [ "$1" = 'up-glob2' ]; then 
    python3 ./shipyard_databricks_sql/cli/upload.py --access-token $DATABRICKS_SQL_ACCESS_TOKEN \
    --server-host $DATABRICKS_SERVER_HOST \
    --http-path $DATABRICKS_HTTP_PATH \
    --catalog $DEMO_CATALOG \
    --schema $DEMO_SCHEMA \
    --volume $DEMO_VOLUME \
    --table-name "pytest_glob_upload" \
    --insert-method "replace" \
    --file-type "csv" \
    --file-name "*.csv" \
    --match-type "glob_match"  \
    --folder-name "mult"

fi


 


