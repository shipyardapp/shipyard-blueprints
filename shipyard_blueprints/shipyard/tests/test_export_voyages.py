import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(find_dotenv())

file_name = "pytest_logs.csv"
dir_name = "pytest_logs"


@pytest.fixture(scope="module")
def creds():
    return {
        "token": os.getenv("SHIPYARD_API_TOKEN"),
        "org": os.getenv("ORG_ID"),
    }


@pytest.fixture(scope="module")
def command(creds):
    return [
        "python3",
        "./shipyard_api/cli/get_logs.py",
        "--api-key",
        creds["token"],
        "--organization-id",
        creds["org"],
    ]


def test_get_logs_no_folder(command):
    cmd = deepcopy(command)
    cmd.extend(["--file-name", file_name])
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_get_logs_with_folder(command):
    cmd = deepcopy(command)
    cmd.extend(["--file-name", file_name, "--folder-name", dir_name])
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0
    assert os.path.exists(dir_name)


def test_folder_exists():
    assert os.path.exists(dir_name)


def test_cleanup():
    os.remove(file_name)
    subprocess.run(["rm", "-fr", dir_name])
    assert not os.path.exists(dir_name)
