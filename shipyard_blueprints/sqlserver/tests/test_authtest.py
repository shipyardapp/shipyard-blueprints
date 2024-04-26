import os
import pytest
import subprocess
from dotenv import load_dotenv, find_dotenv
from shipyard_sqlserver import SqlServerClient
from shipyard_sqlserver.exceptions import SqlServerConnectionError
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("MSSQL_HOST"),
        "pwd": os.getenv("MSSQL_PASSWORD"),
        "user": os.getenv("MSSQL_USERNAME"),
        "db": os.getenv("MSSQL_DATABASE"),
        "port": os.getenv("MSSQL_PORT"),
    }


@pytest.fixture(scope="module")
def cmd():
    return ["python3", "./shipyard_sqlserver/cli/authtest.py"]


def test_good_connection(cmd):
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_bad_host(cmd, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "bad_host")
    process = subprocess.run(cmd)
    assert process.returncode == 1


def test_bad_user(cmd, monkeypatch):
    monkeypatch.setenv("MSSQL_USERNAME", "bad_user")
    process = subprocess.run(cmd)
    assert process.returncode == 1


def test_bad_pwd(cmd, monkeypatch):
    monkeypatch.setenv("MSSQL_PASSWORD", "bad_pwd")
    process = subprocess.run(cmd)
    assert process.returncode == 1
