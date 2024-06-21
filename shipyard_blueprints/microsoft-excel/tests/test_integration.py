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
        "./shipyard_excel/cli/upload.py",
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
        "./shipyard_excel/cli/download.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


def test_upload(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--file-name",
            "xl.csv",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0
