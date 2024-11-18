import pytest
import os
import subprocess

from dotenv import load_dotenv, find_dotenv
from shipyard_magnite import MagniteClient
from pytest import MonkeyPatch
from copy import deepcopy
from requests.exceptions import HTTPError

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def command():
    return [
        "python3",
        "./shipyard_magnite/cli/authtest.py",
    ]


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
