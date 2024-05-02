set -o allexport 
source .env set +o allexport

if [ "$1" = 'up1' ]; then 
    python3 ./shipyard_motherduck/cli/upload.py \
        --token $MOTHERDUCK_TOKEN \
        --table-name "pytest_csv_upload" \
        --file-name "test.csv" \
        --insert-method "replace" \
        --match-type "exact_match"
fi


if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_motherduck/cli/fetch.py \
        --token $MOTHERDUCK_TOKEN \
        --query "select * from pytest_csv_upload" \
        --file-name "download.csv" \
        --file-type "csv"

fi

if [ "$1" = 'up2' ]; then 
    python3 ./shipyard_motherduck/cli/upload.py \
        --token $MOTHERDUCK_TOKEN \
        --table-name "pytest_csv_multiple_upload" \
        --file-name "*.csv" \
        --directory "mult" \
        --insert-method "replace" \
        --match-type "glob_match"
fi

if [ "$1" = 'ex1' ]; then 
    python3 ./shipyard_motherduck/cli/execute.py \
        --token $MOTHERDUCK_TOKEN \
        --query "drop table pytest_parquet_multiple_upload"
fi


if [ "$1" = 'up3' ]; then 
    python3 ./shipyard_motherduck/cli/upload.py \
        --token $MOTHERDUCK_TOKEN \
        --table-name "new_test_table" \
        --file-name "test.csv" \
        --insert-method "append" \
        --match-type "exact_match"

fi


if [ "$1" = 'dl2' ]; then   
    python3 ./shipyard_motherduck/cli/fetch.py \
        --token $MOTHERDUCK_TOKEN \
        --query "select * from stg_upload" \
        --file-name "results.csv" \
        --file-type "csv" \
        --database "staging"
fi
    
