set -o allexport 
source .env set +o allexport

if [ "$1" = 'up1' ]; then 
    python3 ./shipyard_microsoft_onedrive/cli/onedrive_upload.py \
        --client-id "1a0ad857-b07a-4be3-8c0b-5ade79bd573e" \
        --client-secret "sKO8Q~1WZ8NQZT53fSRNEChkxYOOjRFx30qRvcyR" \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --file-name "test.csv" \
        --user-email "bpuser@shipyardapp.onmicrosoft.com" \
        --onedrive-directory "up_test"
        
        # --client-secret $MS_ONEDRIVE_CLIENT_SECRET_VALUE \

fi

if [ "$1" = 'rm1' ]; then 
    python3 ./shipyard_microsoft_onedrive/cli/onedrive_remove.py \
        --client-id "1a0ad857-b07a-4be3-8c0b-5ade79bd573e" \
        --client-secret "sKO8Q~1WZ8NQZT53fSRNEChkxYOOjRFx30qRvcyR" \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --user-email "bpuser@shipyardapp.onmicrosoft.com" \
        --onedrive-file-name "test.csv"
fi


if [ "$1" = 'dl1' ]; then 
    python3 ./shipyard_microsoft_onedrive/cli/onedrive_download.py \
        --client-id "1a0ad857-b07a-4be3-8c0b-5ade79bd573e" \
        --client-secret "sKO8Q~1WZ8NQZT53fSRNEChkxYOOjRFx30qRvcyR" \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --user-email "bpuser@shipyardapp.onmicrosoft.com" \
        --onedrive-file-name "test.csv" \
        --file-name "downloaded.csv"
fi

if [ "$1" = 'mv1' ]; then 
    python3 ./shipyard_microsoft_onedrive/cli/onedrive_move.py \
        --client-id $MS_ONEDRIVE_CLIENT_ID \
        --client-secret $MS_ONEDRIVE_CLIENT_SECRET_VALUE \
        --tenant $MS_ONEDRIVE_TENANT_ID \
        --src-file "test.csv" \
        --dest-dir "move_test_2" \
        --user-email "bpuser@shipyardapp.onmicrosoft.com"
fi
