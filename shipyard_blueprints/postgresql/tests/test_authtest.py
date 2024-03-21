import os
import pytest
import subprocess
from shipyard_postgresql import PostgresClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "pwd": os.getenv("POSTGRES_PASSWORD"),
        "user": os.getenv("POSTGRES_USERNAME"),
        "db": os.getenv("POSTGRES_DATABASE"),
        "port": os.getenv("POSTGRES_PORT"),
    }


@pytest.fixture(scope="module")
def cmd():
    return ["python3", "./shipyard_postgresql/cli/authtest.py"]


def test_good_connection(cmd):
    process = subprocess.run(cmd)
    assert process.returncode == 0
