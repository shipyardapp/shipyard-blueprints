import os
import subprocess
import tempfile
import time

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_ftp.exceptions import EXIT_CODE_FTP_DELETE_ERROR
from shipyard_ftp.ftp import FtpClient

RUN_COMMAND = ["python3", "shipyard_ftp/cli/delete.py"]


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


@pytest.fixture
def create_test_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test.txt")

        with open(file_path, "w") as f:
            f.write("Some test content")

        yield file_path


@pytest.fixture
def upload_test_file(create_test_file, request):
    filename = request.param
    file_path = create_test_file
    FtpClient(
        host=os.getenv("FTP_HOST"),
        port=os.getenv("FTP_PORT"),
        user=os.getenv("FTP_USERNAME"),
        pwd=os.getenv("FTP_PASSWORD"),
    ).upload(file_path, filename)
    time.sleep(5)


@pytest.mark.parametrize("upload_test_file", ["test.txt"], indirect=True)
def test_valid_simple_delete(upload_test_file, setup):
    connection_args = setup

    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            "test.txt",
            "--source-folder-name",
            "",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"File deletion failed: {test_run.returncode} {test_run.stdout} {test_run.stderr}"


@pytest.mark.parametrize("upload_test_file", ["pytest/test.txt"], indirect=True)
def test_simple_nested_delete(upload_test_file, setup):
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            "test.txt",
            "--source-folder-name",
            "pytest",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"File deletion failed: {test_run.returncode} {test_run.stdout} {test_run.stderr}"


def test_invalid_filename(setup):
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            "thisfiledoesnotexist.txt",
            "--source-folder-name",
            "",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == EXIT_CODE_FTP_DELETE_ERROR
    ), f"File deletion failed: {test_run.returncode} {test_run.stdout} {test_run.stderr}"


def test_invalid_credentials():
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
            "--source-folder-name",
            "",
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == FtpClient.EXIT_CODE_INVALID_CREDENTIALS
    ), f"File deletion failed: {test_run.returncode} {test_run.stdout} {test_run.stderr}"
