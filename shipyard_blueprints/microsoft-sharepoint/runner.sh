set -o allexport 
source .env set +o allexport

if [ $1 = 'up1' ]; then 
    echo "Uploading file to Microsoft SharePoint"
    python3 ./shipyard_microsoft_sharepoint/cli/upload.py \
        --client-id $SHAREPOINT_CLIENT_ID \
        --client-secret $SHAREPOINT_CLIENT_SECRET_VALUE \
        --tenant $SHAREPOINT_TENANT_ID \
        --site-name $SHAREPOINT_SITE_ID \
        --file-name "sharepoint_test.csv" 
fi


if [ $1 = 'dl1' ]; then 
    echo "Downloading file from Microsoft SharePoint"
    python3 ./shipyard_microsoft_sharepoint/cli/download.py \
        --client-id $SHAREPOINT_CLIENT_ID \
        --client-secret $SHAREPOINT_CLIENT_SECRET_VALUE \
        --tenant $SHAREPOINT_TENANT_ID \
        --site-name $SHAREPOINT_SITE_ID \
        --sharepoint-file-name "sharepoint_test.csv" \
        --site-name $SHAREPOINT_SITE_ID
fi


if [ "$1" = 'mv1' ]; then 
    echo "Renaming file in Microsoft SharePoint"
    python3 ./shipyard_microsoft_sharepoint/cli/move.py \
        --client-id $SHAREPOINT_CLIENT_ID \
        --client-secret $SHAREPOINT_CLIENT_SECRET_VALUE \
        --tenant $SHAREPOINT_TENANT_ID \
        --site-name $SHAREPOINT_SITE_ID \
        --src-file "sharepoint_test.csv" \
        --dest-file "renamed_test.csv"
fi


if [ "$1" = 'rm1' ]; then 
    echo "Removing file in Microsoft SharePoint"
    python3 ./shipyard_microsoft_sharepoint/cli/remove.py \
        --client-id $SHAREPOINT_CLIENT_ID \
        --client-secret $SHAREPOINT_CLIENT_SECRET_VALUE \
        --tenant $SHAREPOINT_TENANT_ID \
        --site-name $SHAREPOINT_SITE_ID \
        --sharepoint-file-name "renamed_test.csv"
fi


if [ "$1" = 'up2' ]; then 
    python3 ./shipyard_microsoft_sharepoint/cli/upload.py \
        --client-id $SHAREPOINT_CLIENT_ID \
        --client-secret $SHAREPOINT_CLIENT_SECRET_VALUE \
        --tenant $SHAREPOINT_TENANT_ID \
        --site-name $SHAREPOINT_SITE_ID \
        --match-type "regex_match" \
        --file-name "mult" \
        --directory "mult" \
        --sharepoint-directory "pytest_regex_upload"
fi





