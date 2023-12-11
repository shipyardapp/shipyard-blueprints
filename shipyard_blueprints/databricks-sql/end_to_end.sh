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
    --match-type "regex_match"
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
    --catalog $CATALOG2 \
    --schema $SCHEMA2 \
    --table-name "parquet_test" \
    --insert-method "replace" \
    --file-type "parquet" \
    --file-name "parquet" \
    --match-type "regex_match"
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


 


