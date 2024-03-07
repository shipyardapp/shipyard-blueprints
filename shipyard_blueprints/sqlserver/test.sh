set -o allexport 
source .env set +o allexport



if [ "$1" = 'up1' ]; then 
    python3 ./shipyard_sqlserver/cli/upload_file.py \
        --username $USER \
        --password $PWD \
        --host $HOST \
        --database $DB \
        --port 1433 \
        --source-file-name "test.csv" \
        --table-name $UP_TABLE \
        --insert-method "replace"

fi

if [ "$1" = 'ex1' ]; then 
    python3 ./shipyard_sqlserver/cli/execute_sql.py \
        --username $SQL_USER \
        --password $SQL_PWD \
        --host $SQL_HOST \
        --database $SQL_DB \
        --query "drop table $UP_TABLE"

fi


if [ "$1" = 'ex2' ]; then 
    python3 ./shipyard_sqlserver/cli/execute_sql.py \
        --username $SQL_USER \
        --password $SQL_PWD \
        --host $SQL_HOST \
        --database $SQL_DB \
        --query "drop table $REGEX_TABLE"

fi


