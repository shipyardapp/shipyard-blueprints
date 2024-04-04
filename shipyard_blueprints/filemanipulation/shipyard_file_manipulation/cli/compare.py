import pandas as pd
import argparse
import os
import sys
import shipyard_bp_utils as shipyard
from shipyard_templates import ShipyardLogger
from shipyard_file_manipulation.errors import (
    EXIT_CODE_UNKNOWN_ERROR,
    EXIT_CODE_FILE_NOT_FOUND,
)


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name2", dest="source_file_name2", required=True)
    parser.add_argument(
        "--source-folder-name2", dest="source_folder_name2", default="", required=False
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        src_file = args.source_file_name
        src_dir = shipyard.files.clean_folder_name(args.source_folder_name)
        src_path = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir, file_name=src_file
        )

        src_file_2 = args.source_file_name2
        src_dir_2 = shipyard.files.clean_folder_name(args.source_folder_name2)
        src_path_2 = shipyard.files.combine_folder_and_file_name(
            folder_name=src_dir_2, file_name=src_file_2
        )

        df1 = pd.read_csv(src_path)
        df2 = pd.read_csv(src_path_2)

        merged_df = df1.merge(df2, how="outer", indicator=True)
        df1_only = merged_df.loc[lambda x: x["_merge"] == "left_only"].drop(
            columns=["_merge"]
        )
        df2_only = merged_df.loc[lambda x: x["_merge"] == "right_only"].drop(
            columns=["_merge"]
        )
        df_overlap = merged_df.loc[lambda x: x["_merge"] == "both"].drop(
            columns=["_merge"]
        )

        if len(df1_only) > 0:
            df1_only_name = f"{os.path.splitext(src_file)[0]}_only.csv"
            df1_only.to_csv(df1_only_name, index=False, chunksize=10000)
            logger.info(f"Created {df1_only_name}")
        if len(df2_only) > 0:
            df2_only_name = f"{os.path.splitext(src_file_2)[0]}_only.csv"
            df2_only.to_csv(df2_only_name, index=False, chunksize=10000)
            logger.info(f"Created {df2_only_name}")
        if len(df_overlap) > 0:
            overlap_name = f"{os.path.splitext(src_file)[0]}_overlap.csv"
            df_overlap.to_csv(overlap_name, index=False, chunksize=10000)
            logger.info(f"Created {overlap_name}")
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to compare files: {e}"
        )
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
