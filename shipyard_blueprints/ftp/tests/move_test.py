import os
import subprocess
import tempfile

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_ftp.ftp import FtpClient

RUN_COMMAND = ["python3", "shipyard_ftp/cli/move.py"]


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


def test_valid_simple_move(create_test_file, setup):
    file_path = create_test_file
    FtpClient(
        host=os.getenv("FTP_HOST"),
        port=os.getenv("FTP_PORT"),
        user=os.getenv("FTP_USERNAME"),
        pwd=os.getenv("FTP_PASSWORD"),
    ).upload(file_path, "test.txt")

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
