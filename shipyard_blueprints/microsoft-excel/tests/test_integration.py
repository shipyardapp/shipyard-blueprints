import pytest
import os
import pytest
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def upload():
    return [
        "python3",
        "./shipyard_microsoft_excel/cli/upload.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


@pytest.fixture(scope="module")
def download():
    return [
        "python3",
        "./shipyard_microsoft_excel/cli/download.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


def test_upload_csv(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--file-name",
            "xl.csv",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_upload_xl_rename(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--file-name",
            "xl.xlsx",
            "--onedrive-file-name",
            "pytest_xl.xlsx",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_xl_rename(download):
    cmd = deepcopy(download)
    cmd.extend(
        [
            "--file-name",
            "pytest_xl_dl.xlsx",
            "--onedrive-file-name",
            "pytest_xl.xlsx",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_xl_to_new_dir(download):
    cmd = deepcopy(download)
    cmd.extend(
        [
            "--onedrive-file-name",
            "pytest_xl.xlsx",
            "--directory",
            "pytest_dir",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_exists():
    assert os.path.exists("pytest_dir/pytest_xl.xlsx")
    assert os.path.exists("pytest_xl_dl.xlsx")

    os.remove("pytest_xl_dl.xlsx")
    subprocess.run(["rm", "-rf", "pytest_dir"])
