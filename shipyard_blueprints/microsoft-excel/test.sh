set -o allexport 
source .env set +o allexport

if [ "$1" = 'up1' ]; then 
    python3 ./shipyard_excel/cli/upload.py \
        --client-id  $MS_ONEDRIVE_CLIENT_ID \
        --client-secret $MS_ONEDRIVE_CLIENT_SECRET_VALUE \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --file-name "xl.csv" \
        --user-email $MS_ONEDRIVE_EMAIL 
fi

if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_excel/cli/download.py \
        --client-id  $MS_ONEDRIVE_CLIENT_ID \
        --client-secret $MS_ONEDRIVE_CLIENT_SECRET_VALUE \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --onedrive-file-name "xltest.xlsx" \
        --user-email $MS_ONEDRIVE_EMAIL \
        --sheet-name "data"
fi
