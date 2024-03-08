set -o allexport 
source .env set +o allexport

KEY=$AWS_ACCESS_KEY_ID # for brevity
SECRET=$AWS_SECRET_ACCESS_KEY # for brevity
# directories for each program
UP="./shipyard_s3/cli/upload.py"
DOWN="./shipyard_s3/cli/download.py"
MOVE="./shipyard_s3/cli/move.py"
REMOVE="./shipyard_s3/cli/remove.py"


if [ "$1" = 'int1' ]; then
    original_rows=$(awk 'END {print NR}' s3.csv)
    echo "original_rows is $original_rows"
    echo "Beginning integration test to load a single file to S3 then, then download it and compare to original"
    python3 $UP --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $REGION \
        --bucket-name $BUCKET \
        --source-file-name-match-type 'exact_match' \
        --source-file-name 's3.csv' \
        --destination-folder-name $S3_FOLDER 

    if [ $? -eq 0 ]; then 
        echo "Successfully uploaded file to S3"
    else 
        echo  "Error in Uploading file to S3" 
        exit $?
    fi 
    # move on to downloading the target file
    echo "Beginning to download the recently uploaded file"
    python3 $DOWN --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $REGION \
        --bucket-name $BUCKET \
        --source-folder-name $S3_FOLDER \
        --source-file-name 's3.csv' \
        --source-file-name-match-type 'exact_match' \
        --destination-file-name "new_s3.csv" 

    new_rows=$(awk 'END {print NR}' new_s3.csv)
    echo "new rows is $new_rows"

    if [ $? -eq 0 ]; then 
        echo "Successfully downloaded file from S3"
    else 
        echo  "Error in Downloading file from S3" 
        exit $?
    fi 
    
    if [ "$original_rows" = "$new_rows" ]; then 
        echo "Files match! Removing downloaded..." 
        rm new_s3.csv
    else
        echo "Files do not match"
        exit 1
    fi
fi





 







