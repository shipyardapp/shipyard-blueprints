import pandas as pd
import argparse
import os
import code


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


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip("/")
    if folder_name != "":
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}'
    )
    combined_name = os.path.normpath(combined_name)

    return combined_name


def main():
    args = get_args()
    source_file_name = args.source_file_name
    source_folder_name = clean_folder_name(args.source_folder_name)
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name
    )

    source_file_name2 = args.source_file_name2
    source_folder_name2 = clean_folder_name(args.source_folder_name2)
    source_full_path2 = combine_folder_and_file_name(
        folder_name=source_folder_name2, file_name=source_file_name2
    )

    df1 = pd.read_csv(source_full_path)
    df2 = pd.read_csv(source_full_path2)

    merged_df = df1.merge(df2, how="outer", indicator=True)
    df1_only = merged_df.loc[lambda x: x["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )
    df2_only = merged_df.loc[lambda x: x["_merge"] == "right_only"].drop(
        columns=["_merge"]
    )
    df_overlap = merged_df.loc[lambda x: x["_merge"] == "both"].drop(columns=["_merge"])

    if len(df1_only) > 0:
        df1_only_name = f"{os.path.splitext(source_file_name)[0]}_only.csv"
        df1_only.to_csv(df1_only_name, index=False, chunksize=10000)
        print(f"Created {df1_only_name}")
    if len(df2_only) > 0:
        df2_only_name = f"{os.path.splitext(source_file_name2)[0]}_only.csv"
        df2_only.to_csv(df2_only_name, index=False, chunksize=10000)
        print(f"Created {df2_only_name}")
    if len(df_overlap) > 0:
        overlap_name = f"{os.path.splitext(source_file_name)[0]}_overlap.csv"
        df_overlap.to_csv(overlap_name, index=False, chunksize=10000)
        print(f"Created {overlap_name}")


if __name__ == "__main__":
    main()
