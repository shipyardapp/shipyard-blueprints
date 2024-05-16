import os
import subprocess

import pytest
from dotenv import load_dotenv, find_dotenv


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in [
                "SNOWFLAKE_USERNAME",
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_ACCOUNT",
            ]
    ):
        pytest.skip("Missing one or more required environment variables")


@pytest.fixture(scope="module")
def auth_cmd():
    return ["python3", "shipyard_snowflake/cli/authtest.py"]


def test_auth_success(auth_cmd, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY")
    result = subprocess.run(auth_cmd)
    assert result.returncode == 0


def test_auth_bad_user(auth_cmd, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USERNAME", "bad_user")
    result = subprocess.run(auth_cmd)
    assert result.returncode == 1
