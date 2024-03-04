import subprocess
import pytest
import os
import glob
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from shipyard_domo import DomoClient

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def domo_credentials():
    return {
        "DOMO_CLIENT_ID": os.getenv("DOMO_CLIENT_ID"),
        "DOMO_SECRET_KEY": os.getenv("DOMO_SECRET_KEY"),
        "DOMO_ACCESS_TOKEN": os.getenv("DOMO_ACCESS_TOKEN"),
    }


@pytest.fixture(scope="module")
def domo_datasets():
    return {
        "DS_REFRESH": os.getenv("DS_REFRESH"),
        "DS_DOWNLOAD": os.getenv("DS_DOWNLOAD"),
        "DS_DL_BASE_CASE": os.getenv("DS_DL_BASE_CASE"),
    }


def upload_regex_from_folder(domo_credentials):
    ds = os.getenv("REGEX_MATCH_DS")
    upload_cmd = [
        "python3",
        "./shipyard_domo/cli/upload.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--dataset-name",
        "Regex Match Upload",
        "--dataset-description",
        "This is a base case for regex match upload",
        "--insert-method",
        "REPLACE",
        "--source-file-match-type",
        "regex_match",
        "--file-name",
        "mult",
        "--folder-name",
        "nested_upload",
        "--dataset-id",
        ds,
    ]

    subprocess.run(upload_cmd, check=True)


def test_download(domo_credentials):
    upload_regex_from_folder(domo_credentials)
    ds = os.getenv("REGEX_MATCH_DS")
    down_cmd = [
        "python3",
        "./shipyard_domo/cli/download_dataset.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--destination-file-name",
        "regex_match_upload.csv",
        "--destination-folder-name",
        "nested_upload",
        "--dataset-id",
        ds,
    ]

    subprocess.run(down_cmd)

    # check that the downloaded file matches the original 3 files
    files = glob.glob("nested_upload/mult_*")
    dfs = []
    for f in files:
        df = pd.read_csv(
            f,
        )
        dfs.append(df)
    original = pd.concat(dfs, axis=0, ignore_index=True)
    downloaded_file = pd.read_csv("nested_upload/regex_match_upload.csv")

    assert original.shape[0] == downloaded_file.shape[0]

    os.remove("nested_upload/regex_match_upload.csv")
    print("Removed downloaded file")


def test_upload_with_schema_replace(domo_credentials):
    ds = os.getenv("DOMO_DATASET_LARGE")
    upload_cmd = [
        "python3",
        "./shipyard_domo/cli/upload.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--insert-method",
        "REPLACE",
        "--source-file-match-type",
        "exact_match",
        "--file-name",
        "test.csv",
        "--dataset-name",
        "Upload With Schema",
        "--domo-schema",
        "[['string_col','string'],['char_col','string'],['int_col','long'],['float_col','double'],['bool_col','string'],['date_col','datetime'],['datetime_col','datetime']]",
        "--dataset-id",
        ds,
    ]

    process = subprocess.run(upload_cmd, check=True)
    assert process.returncode == 0


def test_upload_download_with_schema_append(domo_credentials):
    ds = os.getenv("DOMO_DATASET_LARGE")
    upload_cmd = [
        "python3",
        "./shipyard_domo/cli/upload.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--insert-method",
        "APPEND",
        "--source-file-match-type",
        "exact_match",
        "--file-name",
        "test.csv",
        "--dataset-name",
        "Upload With Schema",
        "--domo-schema",
        "[['string_col','string'],['char_col','string'],['int_col','long'],['float_col','double'],['bool_col','string'],['date_col','datetime'],['datetime_col','datetime']]",
        "--dataset-id",
        ds,
    ]

    process = subprocess.run(upload_cmd, check=True)
    assert process.returncode == 0


def test_download_after_append(domo_credentials):
    ds = os.getenv("DOMO_DATASET_LARGE")
    original_file = "test.csv"
    down_cmd = [
        "python3",
        "./shipyard_domo/cli/download_dataset.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--destination-file-name",
        "larger.csv",
        "--dataset-id",
        ds,
    ]

    subprocess.run(down_cmd)
    orig_df = pd.read_csv(original_file)
    new_df = pd.read_csv("larger.csv")
    assert (2 * orig_df.shape[0]) == new_df.shape[0]

    os.remove("larger.csv")


def test_download_card(domo_credentials):
    down_cmd = [
        "python3",
        "./shipyard_domo/cli/export_card_to_file.py",
        "--card-id",
        os.getenv("DOMO_CARD_ID"),
        "--destination-file-name",
        "card.csv",
        "--destination-folder-name",
        "cards",
        "--file-type",
        "csv",
        "--developer-token",
        domo_credentials["DOMO_ACCESS_TOKEN"],
        "--domo-instance",
        os.getenv("DOMO_INSTANCE"),
    ]
    process = subprocess.run(down_cmd)

    assert process.returncode == 0
