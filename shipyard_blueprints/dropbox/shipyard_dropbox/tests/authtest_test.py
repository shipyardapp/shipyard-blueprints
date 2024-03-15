import os
import subprocess

import pytest
from dotenv import load_dotenv

RUN_COMMAND = ["python3", "shipyard_dropbox/cli/authtest.py"]


@pytest.fixture(scope="module", autouse=True)
def load_env():
    load_dotenv()


def test_valid_credentials():
    if not os.getenv("DROPBOX_ACCESS_TOKEN"):
        pytest.skip("No Dropbox credentials found")

    test = subprocess.run(RUN_COMMAND)
    assert test.returncode == 0


def test_invalid_credentials():
    test = subprocess.run(
        RUN_COMMAND,
        env={"DROPBOX_ACCESS_TOKEN": "invalid"},
        capture_output=True,
        text=True,
    )
    assert test.returncode == 1


def test_no_credentials():
    test = subprocess.run(RUN_COMMAND, capture_output=True, text=True, env={})
    assert test.returncode == 1


def test_empty_string_credentials():
    test = subprocess.run(
        RUN_COMMAND, capture_output=True, text=True, env={"DROPBOX_ACCESS_TOKEN": ""}
    )
    assert test.returncode == 1
