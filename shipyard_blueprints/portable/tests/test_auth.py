import os
import subprocess
import pytest
from shipyard_portable import PortableClient
from dotenv import load_dotenv, find_dotenv

if env_exists := os.path.exists(".env"):
    load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def auth():
    return ["python3", "./shipyard_portable/cli/authtest.py"]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_good_connection():
    access_token = os.getenv("PORTABLE_API_TOKEN")
    portable = PortableClient(access_token)
    assert portable.connect() == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_bad_connection():
    access_token = "bad_token"
    portable = PortableClient(access_token)
    assert portable.connect() == 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_authtest_good_connection(auth):
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_authtest_bad_connection(auth, monkeypatch):
    monkeypatch.setenv("PORTABLE_API_TOKEN", "bad_token")
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 1
