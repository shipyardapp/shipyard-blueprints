import pytest
import subprocess
from dotenv import load_dotenv, find_dotenv


if env_exists := find_dotenv():
    load_dotenv()


@pytest.fixture(scope="module")
def authtest():
    return ["python3", "./shipyard_motherduck/cli/authtest.py"]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_authtest_good_connection(authtest):
    result = subprocess.run(authtest, capture_output=True)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_authtest_bad_connection(authtest, monkeypatch):
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "bad_token")
    result = subprocess.run(authtest, capture_output=True)
    print(result.stdout)
    assert result.returncode == 1
