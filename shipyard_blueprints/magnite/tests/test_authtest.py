import pytest
import subprocess

from dotenv import load_dotenv, find_dotenv
from pytest import MonkeyPatch
from requests.exceptions import HTTPError

env_file_found = load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def command():
    return [
        "python3",
        "./shipyard_magnite/cli/authtest.py",
    ]


@pytest.mark.skipif(not env_file_found, reason="No .env file found")
def test_successful_connection(command):
    result = subprocess.run(command)
    assert result.returncode == 0


def test_unsuccessful_connection_bad_username(command, monkeypatch):
    monkeypatch.setenv("MAGNITE_USERNAME", "bad_user")
    result = subprocess.run(command)
    assert result.returncode == 1


def test_unsuccessful_connection_bad_password(command, monkeypatch):
    monkeypatch.setenv("MAGNITE_PASSWORD", "bad_pwd")
    result = subprocess.run(command)
    assert result.returncode == 1
