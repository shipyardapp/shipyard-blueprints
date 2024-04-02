import os
import subprocess
import pytest
from shipyard_portable import PortableClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def auth():
    return ["python3", "./shipyard_portable/cli/authtest.py"]


def test_good_connection():
    access_token = os.getenv("PORTABLE_API_TOKEN")
    portable = PortableClient(access_token)
    assert portable.connect() == 0


def test_bad_connection():
    access_token = "bad_token"
    portable = PortableClient(access_token)
    assert portable.connect() == 1


def test_authtest_good_connection(auth):
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 0


def test_authtest_bad_connection(auth):
    os.environ["PORTABLE_API_TOKEN"] = "bad_token"
    result = subprocess.run(auth, capture_output=True)
    assert result.returncode == 1
