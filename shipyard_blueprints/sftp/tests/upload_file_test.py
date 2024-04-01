import os
import subprocess
import tempfile

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_sftp.sftp import SftpClient

RUN_COMMAND = ["python3", "shipyard_sftp/cli/upload_file.py"]


@pytest.fixture(scope="module", autouse=True)
def setup():
    load_dotenv(find_dotenv())
    if any(
        key not in os.environ
        for key in [
            "SFTP_HOST",
            "SFTP_PORT",
            "SFTP_USERNAME",
            "SFTP_PASSWORD",
        ]
    ):
        pytest.skip("Missing one or more required environment variables")

    return [
        "--host",
        os.getenv("SFTP_HOST"),
        "--port",
        os.getenv("SFTP_PORT"),
        "--username",
        os.getenv("SFTP_USERNAME"),
        "--password",
        os.getenv("SFTP_PASSWORD"),
    ]


@pytest.fixture
def create_test_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test.txt")

        with open(file_path, "w") as f:
            f.write("Some test content")

        yield file_path


def test_valid_simple_upload(setup):
    content = "Testing simple upload"
    filename = "test_valid_simple_upload.txt"
    with open(filename, "w") as f:
        f.write(content)
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            os.path.basename(filename),
            "--source-folder-name",
            os.path.dirname(filename),
        ],
        text=True,
        capture_output=True,
    )

    assert (
        test_run.returncode == 0
    ), f"{test_run.returncode} {test_run.stdout} {test_run.stderr}"
    server_content = (
        SftpClient(
            host=os.getenv("SFTP_HOST"),
            port=os.getenv("SFTP_PORT"),
            user=os.getenv("SFTP_USERNAME"),
            pwd=os.getenv("SFTP_PASSWORD"),
        )
        .client.open(filename)
        .read()
        .decode()
    )
    assert server_content == content

    os.remove(filename)


def test_simple_nested_upload(setup):
    content = "Testing nested upload"
    filename = "subfolder/test_simple_nested_upload.txt"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        f.write(content)
    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            os.path.basename(filename),
            "--source-folder-name",
            os.path.dirname(filename),
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
    ), f"{test_run.returncode} {test_run.stdout} {test_run.stderr}"
    server_content = (
        SftpClient(
            host=os.getenv("SFTP_HOST"),
            port=os.getenv("SFTP_PORT"),
            user=os.getenv("SFTP_USERNAME"),
            pwd=os.getenv("SFTP_PASSWORD"),
        )
        .client.open("pytest/test.txt")
        .read()
        .decode()
    )
    assert server_content == content

    os.remove(filename)
    os.rmdir(os.path.dirname(filename))


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
        test_run.returncode == SftpClient.EXIT_CODE_FILE_NOT_FOUND
    ), f"{test_run.returncode} {test_run.stdout} {test_run.stderr}"


def test_invalid_credentials(create_test_file):
    with open("test.txt", "w") as f:
        f.write("Some test content")
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
        test_run.returncode == SftpClient.EXIT_CODE_INVALID_CREDENTIALS
    ), f"{test_run.returncode} {test_run.stdout} {test_run.stderr}"
    os.remove("test.txt")
