import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(".env.sheets.oauth")


@pytest.fixture(scope="module")
def upload():
    return [
        "python3",
        "./shipyard_googlesheets/cli/upload.py",
        "--drive",
        "Compatibility",
        "--destination-file-name",
        os.getenv("SHEET_ID"),
    ]


@pytest.fixture(scope="module")
def download():
    return [
        "python3",
        "./shipyard_googlesheets/cli/download.py",
        "--drive",
        "Compatibility",
        "--source-file-name",
        os.getenv("SHEET_ID"),
        "--destination-file-name",
        "download.csv",
    ]


def test_upload(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--source-file-name",
            "single.csv",
        ]
    )

    process = subprocess.run(cmd, capture_output=True)
    print(process.stderr)
    print(process.stdout)
    assert process.returncode == 0


def test_download(download):
    cmd = deepcopy(download)
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_file_exists():
    assert os.path.exists("download.csv")
    os.remove("download.csv")
