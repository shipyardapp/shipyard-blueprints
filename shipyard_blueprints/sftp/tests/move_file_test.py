import contextlib
import os
import subprocess
import tempfile

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_sftp.sftp import SftpClient

RUN_COMMAND = ["python3", "shipyard_sftp/cli/move_file.py"]


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


def test_valid_simple_move(create_test_file, setup):
    file_path = create_test_file
    SftpClient(
        host=os.getenv("SFTP_HOST"),
        port=os.getenv("SFTP_PORT"),
        user=os.getenv("SFTP_USERNAME"),
        pwd=os.getenv("SFTP_PASSWORD"),
    ).upload(file_path, "test.txt")

    with contextlib.suppress(Exception):  # Delete file in case it already exists
        SftpClient(
            host=os.getenv("SFTP_HOST"),
            port=os.getenv("SFTP_PORT"),
            user=os.getenv("SFTP_USERNAME"),
            pwd=os.getenv("SFTP_PASSWORD"),
        ).remove("pytest/moved_test.csv")

    connection_args = setup
    test_run = subprocess.run(
        [
            *RUN_COMMAND,
            *connection_args,
            "--source-file-name",
            "test.txt",
            "--destination-folder-name",
            "pytest",
            "--destination-file-name",
            "moved_test.csv",
        ],
        text=True,
        capture_output=True,
    )
    assert (
            test_run.returncode == 0
    ), f"File move failed: {test_run.returncode} {test_run.stdout} {test_run.stderr}"
