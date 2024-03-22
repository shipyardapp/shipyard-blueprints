import os
import pytest
import subprocess
from shipyard_mysql import MySqlClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("MYSQL_HOST"),
        "pwd": os.getenv("MYSQL_PASSWORD"),
        "user": os.getenv("MYSQL_USERNAME"),
        "db": os.getenv("MYSQL_DATABASE"),
        "port": os.getenv("MYSQL_PORT"),
    }


@pytest.fixture(scope="module")
def cmd():
    return ["python3", "./shipyard_mysql/cli/authtest.py"]


def test_good_connection(cmd):
    process = subprocess.run(cmd)
    assert process.returncode == 0
