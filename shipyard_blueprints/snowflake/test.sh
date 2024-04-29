set -o allexport 
source .env set +o allexport

if [ "$1" =  'up1' ]; then 
    echo "Starting upload of single file to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "test_logs.csv" \
    --insert-method "replace" \
    --table-name "test_spaced"

fi




if [ "$1" =  'up2' ]; then 
    echo "Starting upload of single file to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "test_logs.csv" \
    --insert-method "replace" \
    --table-name "test_spaced" \
    --snowflake-data-types "[['Fleet ID','STRING'], ['Fleet Name','STRING'], ['Fleet Version','INT'], ['Fleet Log ID','STRING'], ['Fleet Log Trigger','STRING'], ['Fleet Log Status','STRING'], ['Vessel Log ID','STRING'], ['Vessel Status','STRING'], ['Vessel Name','STRING'], ['Vessel Trigger','STRING'], ['Retries','INT'], ['Exit Code','INT'], ['Vessel Start Time','DATETIME'], ['Vessel End Time','DATETIME'], ['Duration','NUMBER'], ['Billable Runtime','NUMBER']]"

fi


if [ "$1" =  'up3' ]; then 
    echo "Starting upload of single file to replace table"
    python3 ./shipyard_snowflake/cli/upload.py --username $SNOWFLAKE_USERNAME \
    --password $SNOWFLAKE_PASSWORD \
    --account $SNOWFLAKE_ACCOUNT \
    --schema $SNOWFLAKE_SCHEMA \
    --database $SNOWFLAKE_DATABASE \
    --warehouse $SNOWFLAKE_WAREHOUSE \
    --source-file-name "snowflake.csv" \
    --insert-method "replace" \
    --table-name "test_local" 

fi

