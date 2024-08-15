import os
import pytest
import subprocess
from copy import deepcopy
from dotenv import load_dotenv

load_dotenv(".env.drive.oauth")


@pytest.fixture
def creds():
    return {
        "service_account": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        "refresh_token": os.environ.get("OAUTH_REFRESH_TOKEN"),
        "drive": os.environ.get("DRIVE_ID"),
    }


@pytest.fixture
def upload():
    return ["python3", "./shipyard_googledrive/cli/upload.py"]


@pytest.fixture
def download():
    return ["python3", "./shipyard_googledrive/cli/download.py"]


def test_upload_single_file(upload, creds):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--source-file-name",
            "test.csv",
            "--drive",
            creds["drive"],
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_single_file(download, creds):
    cmd = deepcopy(download)
    cmd.extend(
        [
            "--source-file-name",
            "test.csv",
            "--drive",
            creds["drive"],
            "--destination-file-name",
            "oauth_downloaded.csv",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_file_exists():
    assert os.path.exists("oauth_downloaded.csv")
    print("File exists, removing now")
    os.remove("oauth_downloaded.csv")
