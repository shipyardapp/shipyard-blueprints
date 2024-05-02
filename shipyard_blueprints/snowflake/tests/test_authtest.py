import subprocess
import pytest
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def auth_cmd():
    return ["python3", "shipyard_snowflake/cli/authtest.py"]


def test_auth_success(auth_cmd):
    result = subprocess.run(auth_cmd)
    assert result.returncode == 0


def test_auth_bad_user(auth_cmd, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USERNAME", "bad_user")
    result = subprocess.run(auth_cmd)
    assert result.returncode == 1
