import subprocess

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_ftp.ftp import FtpClient

RUN_COMMAND = ["python3", "shipyard_ftp/cli/upload.py"]


@pytest.fixture(scope="module", autouse=True)
def setup():
    load_dotenv(find_dotenv())
    if any(
        key not in os.environ
        for key in [
            "FTP_HOST",
            "FTP_PORT",
            "FTP_USERNAME",
            "FTP_PASSWORD",
        ]
    ):
        pytest.skip("Missing one or more required environment variables")

    return [
        "--host",
        os.getenv("FTP_HOST"),
        "--port",
        os.getenv("FTP_PORT"),
        "--username",
        os.getenv("FTP_USERNAME"),
        "--password",
        os.getenv("FTP_PASSWORD"),
    ]


import pytest
import tempfile
import os


@pytest.fixture
def temp_file():
    fd, path = tempfile.mkstemp()
    os.close(fd)

    yield path

    os.remove(path)


def test_valid_simple_upload(setup):
    file_path = "test.txt"
    content = "Some content to write to the file."

    with open(file_path, "w") as test_file:
        test_file.write(content)

    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            os.path.basename(file_path),
            "--source-folder-name",
            os.path.dirname(file_path),
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"{test_run.returncode} {test_run.stdout} {test_run.stderr}"

    os.remove("test.txt")


def test_simple_nested_upload(setup):
    file_path = "sub-folder/test.txt"
    content = "Some content to write to the file."
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as test_file:
        test_file.write(content)

    with open(file_path, "w") as f:
        f.write(content)
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            os.path.basename(file_path),
            "--source-folder-name",
            os.path.dirname(file_path),
            "--source-file-name-match-type",
            "exact_match",
            "--destination-folder-name",
            "pytest",
            "--destination-file-name",
            "test.txt",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"{test_run.returncode} != 0 {test_run.stdout} {test_run.stderr}"

    os.remove(file_path)
    os.rmdir(os.path.dirname(file_path))


def test_simple_nested_upload_with_rename(setup):
    file_path = "sub-folder/test.txt"
    content = "Some content to write to the file."
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as test_file:
        test_file.write(content)

    with open(file_path, "w") as f:
        f.write(content)
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            os.path.basename(file_path),
            "--source-folder-name",
            os.path.dirname(file_path),
            "--source-file-name-match-type",
            "exact_match",
            "--destination-folder-name",
            "pytest",
            "--destination-file-name",
            "new_name_test.txt",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"{test_run.returncode} != 0 {test_run.stdout} {test_run.stderr}"

    os.remove(file_path)
    os.rmdir(os.path.dirname(file_path))


def test_invalid_filename(setup):
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "thisfiledoesnotexist.txt",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == FtpClient.EXIT_CODE_FILE_MATCH_ERROR
    ), f"{test_run.returncode} != {FtpClient.EXIT_CODE_FILE_MATCH_ERROR} {test_run.stdout} {test_run.stderr}"


def test_invalid_credentials():
    file_path = "test.txt"
    content = "Some content to write to the file."

    with open(file_path, "w") as test_file:
        test_file.write(content)

    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            "--host",
            "bad_host",
            "--port",
            "bad_port",
            "--username",
            "bad_username",
            "--password",
            "bad_password",
            "--source-file-name",
            "test.txt",
            "--source-file-name-match-type",
            "exact_match",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == FtpClient.EXIT_CODE_INVALID_CREDENTIALS
    ), f"{test_run.returncode} != {FtpClient.EXIT_CODE_INVALID_CREDENTIALS} {test_run.stdout} {test_run.stderr}"
    os.remove("test.txt")
