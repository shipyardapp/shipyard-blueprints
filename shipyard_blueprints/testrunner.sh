#!/bin/bash

expected_exit_code=$1
project_dir=$2
cli_filename=$3
flags=$4
envfile_path=$5  #

cd "$project_dir" || { echo "Failed to enter project directory $project_dir"; exit 1; }

if [ -f "$envfile_path" ]; then
    set -a
    source "$envfile_path"
    set +a
else
    echo "Environment file $envfile_path not found."
    exit 1
fi

echo "Environment variables loaded from $envfile_path"
echo "client_id: $MICROSOFT_POWER_BI_CLIENT_ID"
echo "Running tests with the following flags: $flags"
echo $flags


poetry run python3 "$cli_filename" $flags

exit_code=$?

if [ "$exit_code" -eq "$expected_exit_code" ]; then
    echo "Success: The exit code ($exit_code) matched the expected exit code."
    exit 0
else
    echo "Error: The exit code ($exit_code) did not match the expected exit code ($expected_exit_code)."
    exit 1
fi
