import subprocess
import pytest
import os
from dotenv import load_dotenv, find_dotenv
from shipyard_domo import DomoClient

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
    assert 1 == 1
