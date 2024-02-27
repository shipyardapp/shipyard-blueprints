import pytest
import os
import subprocess
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def domo_credentials():
    return {
        "DOMO_CLIENT_ID": os.getenv("DOMO_CLIENT_ID"),
        "DOMO_SECRET_KEY": os.getenv("DOMO_SECRET_KEY"),
    }


@pytest.fixture(scope="module")
def domo_datasets():
    return {
        "DS_REFRESH": os.getenv("DS_REFRESH"),
        "DS_DOWNLOAD": os.getenv("DS_DOWNLOAD"),
        "DS_DL_BASE_CASE": os.getenv("DS_DL_BASE_CASE"),
    }


def test_upload_download(domo_credentials, domo_datasets):
    original_file = "soccer.csv"
    upload_cmd = [
        "python3",
        "./shipyard_domo/cli/upload.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--dataset-name",
        "Pytest Base Case",
        "--dataset-description",
        "This is a base case for backwards compatibility",
        "--insert-method",
        "REPLACE",
        "--source-file-match-type",
        "exact_match",
        "--file-name",
        original_file,
        "--dataset-id",
        domo_datasets["DS_DL_BASE_CASE"],
    ]

    output = subprocess.run(upload_cmd, check=True)

    new_file = "base_case_dl.csv"
    download_cmd = [
        "python3",
        "./shipyard_domo/cli/download_dataset.py",
        "--client-id",
        domo_credentials["DOMO_CLIENT_ID"],
        "--secret-key",
        domo_credentials["DOMO_SECRET_KEY"],
        "--dataset-id",
        domo_datasets["DS_DL_BASE_CASE"],
        "--destination-file-name",
        new_file,
    ]

    dl_output = subprocess.run(download_cmd, check=True)

    # verify that the rows counts are the same
    dl_df = pd.read_csv(new_file)
    df_orig = pd.read_csv(original_file)

    assert dl_df.shape[0] == df_orig.shape[0]


def test_download_dataset_to_file(domo_credentials, domo_datasets):
    pass
