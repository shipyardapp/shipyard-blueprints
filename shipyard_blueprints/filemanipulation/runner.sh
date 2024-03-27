if [ "$1" = 'cn1' ]; then 
    python3 ./shipyard_file_manipulation/cli/convert.py \
        "--source-file-name-match-type" "exact_match" \
        "--source-file-name" "test.csv" \
        "--destination-file-name" "converted.h5" \
        "--destination-file-format" "hdf5" 
fi
