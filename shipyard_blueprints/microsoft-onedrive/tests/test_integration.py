import os
import subprocess
import pytest
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())

UPLOAD_1 = "pytest_upload.csv"


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
    cmd = deepcopy(down)
    cmd.extend(
        [
            "--onedrive-file-name",
            "mult",
            "--onedrive-directory",
            "pytest_regex_upload",
            "--match-type",
            "regex_match",
            "--directory",
            "regex_download",
            "--file-name",
            "reg_down",
        ]
    )

    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_regex_download_exists():
    assert os.path.exists("regex_download/reg_down_1.csv")
    assert os.path.exists("regex_download/reg_down_2.csv")
    assert os.path.exists("regex_download/reg_down_3.csv")
    subprocess.run(["rm", "-rf", "regex_download"])


def test_move_regex(move):
    cmd = deepcopy(move)
    cmd.extend(
        [
            "--src-file",
            "mult",
            "--dest-file",
            "new_file",
            "--src-dir",
            "pytest_regex_upload",
            "--dest-dir",
            "pytest_regex_upload",
            "--match-type",
            "regex_match",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_delete_regex(remove):
    cmd = deepcopy(remove)
    cmd.extend(
        [
            "--onedrive-file-name",
            "new_file",
            "--onedrive-directory",
            "pytest_regex_upload",
            "--match-type",
            "regex_match",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_upload_single_file_from_folder(up):
    cmd = deepcopy(up)
    cmd.extend(
        [
            "--file-name",
            "mult_1.csv",
            "--directory",
            "mult",
            "--onedrive-file-name",
            "pytest_upload.csv",
            "--onedrive-directory",
            "pytest_folder_upload",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_single_file_from_folder(down):
    cmd = deepcopy(down)
    cmd.extend(
        [
            "--onedrive-file-name",
            "pytest_upload.csv",
            "--onedrive-directory",
            "pytest_folder_upload",
            "--directory",
            "pytest_folder_download",
            "--file-name",
            "pytest_down.csv",
        ]
    )

    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_single_file_down_exists():
    assert os.path.exists("pytest_folder_download/pytest_down.csv")
    subprocess.run(["rm", "-rf", "pytest_folder_download"])


def test_move_single_file_from_folder_to_root(move):
    cmd = deepcopy(move)
    cmd.extend(
        [
            "--src-file",
            "pytest_upload.csv",
            "--src-dir",
            "pytest_folder_upload",
            "--dest-file",
            "pytest_moved_from_folder.csv",
            "--dest-dir",
            "next_folder_move",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_delete_single_file_from_folder(remove):
    cmd = deepcopy(remove)
    cmd.extend(
        [
            "--onedrive-file-name",
            "pytest_moved_from_folder.csv",
            "--onedrive-directory",
            "next_folder_move",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0
