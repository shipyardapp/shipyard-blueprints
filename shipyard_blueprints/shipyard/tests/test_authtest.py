import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def authtest():
    return ["python3", "shipyard_api/cli/authtest.py"]


def test_authtest(authtest):
    result = subprocess.run(authtest, capture_output=True)
    assert result.returncode == 0


def test_authtest_bad_token(authtest, monkeypatch):
    monkeypatch.setenv("SHIPYARD_API_TOKEN", "bad_token")
    result = subprocess.run(authtest, capture_output=True)
    assert result.returncode == 1
