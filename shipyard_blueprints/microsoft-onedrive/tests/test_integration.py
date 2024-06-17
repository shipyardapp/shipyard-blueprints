import os
import subprocess
import pytest
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())

UPLOAD_1 = "pytest_upload.csv"


# NOTE: NEED to move remove the onedrive directory and just have the clis in the cli dir within that
@pytest.fixture
def up():
    return [
        "python3",
        "./shipyard_microsoft_onedrive/cli/onedrive_upload.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


@pytest.fixture
def down():
    return [
        "python3",
        "./shipyard_microsoft_onedrive/cli/onedrive_download.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


@pytest.fixture
def move():
    return [
        "python3",
        "./shipyard_microsoft_onedrive/cli/onedrive_move.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


@pytest.fixture
def remove():
    return [
        "python3",
        "./shipyard_microsoft_onedrive/cli/onedrive_remove.py",
        "--client-id",
        os.getenv("MS_ONEDRIVE_CLIENT_ID"),
        "--client-secret",
        os.getenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE"),
        "--tenant",
        os.getenv("MS_ONEDRIVE_TENANT_ID"),
        "--user-email",
        "bpuser@shipyardapp.onmicrosoft.com",
    ]


def test_up_no_folder(up):
    cmd = deepcopy(up)
    cmd.extend(
        [
            "--file-name",
            "test.csv",
            "--onedrive-file-name",
            UPLOAD_1,
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_no_folder(down):
    cmd = deepcopy(down)
    cmd.extend(
        [
            "--file-name",
            UPLOAD_1,
            "--onedrive-file-name",
            UPLOAD_1,
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_upload_1_exists():
    assert os.path.exists(UPLOAD_1)
    os.remove(UPLOAD_1)


def test_move_no_folder(move):
    cmd = deepcopy(move)

    cmd.extend(["--src-file", UPLOAD_1, "--dest-file", "newname.csv"])
    process = subprocess.run(cmd, capture_output=True)
    print(process.stderr)
    assert process.returncode == 0


def test_remove_no_folder(remove):
    cmd = deepcopy(remove)
    cmd.extend(["--onedrive-file-name", "newname.csv"])
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_upload_regex_to_folder(up):
    cmd = deepcopy(up)
    cmd.extend(
        [
            "--file-name",
            "mult",
            "--directory",
            "mult",
            "--onedrive-file-name",
            "mult",
            "--onedrive-directory",
            "pytest_regex_upload",
            "--match-type",
            "regex_match",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_regex_to_folder(down):
    pass
