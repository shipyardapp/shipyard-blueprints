import subprocess
import os
import pytest
from dotenv import load_dotenv, find_dotenv


if dotenv_exists := find_dotenv():
    load_dotenv(dotenv_exists)


@pytest.fixture(scope="module")
def auth():
    return ["python3", "shipyard_microsoft_onedrive/cli/authtest.py"]


def test_authtest(auth):
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 0


def test_authtest_bad_client_id(auth, monkeypatch):
    monkeypatch.setenv("MS_ONEDRIVE_CLIENT_ID", "bad_client_id")
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 1


def test_authtest_bad_client_secret(auth, monkeypatch):
    monkeypatch.setenv("MS_ONEDRIVE_CLIENT_SECRET_VALUE", "bad_client_secret")
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 1
