import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

if exists := find_dotenv():
    load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def authtest():
    return ["python3", "./shipyard_tableau/cli/authtest.py"]


def test_access_token_connection(authtest, monkeypatch):
    cmd = deepcopy(authtest)
    monkeypatch.setenv("TABLEAU_SIGN_IN_METHOD", "access_token")
    monkeypatch.setenv("TABLEAU_USERNAME", os.getenv("ACCESS_TOKEN_NAME"))
    monkeypatch.setenv("TABLEAU_PASSWORD", os.getenv("ACCESS_TOKEN_SECRET"))
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_username_password_connection(authtest, monkeypatch):
    cmd = deepcopy(authtest)
    monkeypatch.setenv("TABLEAU_SIGN_IN_METHOD", "username_password")
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0
