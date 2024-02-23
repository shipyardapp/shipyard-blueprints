set -o allexport 
source .env set +o allexport

KEY=$AWS_ACCESS_KEY_ID # for brevity
SECRET=$AWS_SECRET_ACCESS_KEY # for brevity
# directories for each program
UP="./shipyard_s3/cli/upload.py"
DOWN="./shipyard_s3/cli/download.py"
MOVE="./shipyard_s3/cli/move.py"
REMOVE="./shipyard_s3/cli/remove.py"
DOWN_OLD="./shipyard_s3/cli/old/download.py"
UP_OLD="./shipyard_s3/cli/old/upload.py"

if [ "$1" = 'up1' ]; then
    echo "Base case for upload"
    python3 $UP --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $REGION \
        --bucket-name $BUCKET \
        --source-file-name-match-type 'exact_match' \
        --source-file-name 's3.csv' \
        --destination-folder-name $S3_FOLDER 
fi

if [ "$1" = 'up2' ]; then 
    python3 $UP --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $REGION \
        --bucket-name $BUCKET \
        --source-file-name-match-type 'regex_match' \
        --source-folder-name "test_folder" \
        --source-file-name 'data' \
        --destination-folder-name $S3_FOLDER \
        --destination-file-name "regex.csv"
fi

if [ "$1" = 'down1' ]; then
    echo "Beginning to download the recently uploaded file"
    python3 $DOWN --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $BAD_REGION \
        --bucket-name $BAD_BUCKET \
        --source-folder-name $S3_FOLDER \
        --source-file-name 's3.csv' \
        --source-file-name-match-type 'exact_match' \
        --destination-file-name "new_s3.csv"  


    new_rows=$(awk 'END {print NR}' new_s3.csv)
    echo "new rows is $new_rows"

fi



if [ "$1" = 'down2' ]; then
    echo "Beginning to download the recently uploaded file"
    python3 $DOWN --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $REGION \
        --bucket-name $BUCKET \
        --source-folder-name $S3_FOLDER \
        --source-file-name 'regex' \
        --source-file-name-match-type 'regex_match' \
        --destination-folder-name "reg_download" 

fi

if [ "$1" = 'mv1' ]; then
    python3 $MOVE --aws-access-key-id $KEY \
        --aws-secret-access-key $SECRET \
        --aws-default-region $BAD_REGION \
        --source-bucket-name $BUCKET \
        --destination-bucket-name $BUCKET \
        --source-folder-name $S3_FOLDER \
        --source-file-name "s3.csv" \
        --destination-file-name "renamed_s3.csv" \
        --source-file-name-match-type "exact_match"
fi
