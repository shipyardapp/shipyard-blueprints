import pytest
import os
import subprocess
import glob
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def auth():
    return [
        "python3",
        "./shipyard_databricks_sql/cli/authtest.py",
    ]


def test_good_connection(auth):
    process = subprocess.run(auth)

    assert process.returncode == 0


def test_bad_connection_bad_token(auth, monkeypatch):
    monkeypatch.setenv("DATABRICKS_SQL_ACCESS_TOKEN", "bad_token")
    process = subprocess.run(auth)

    assert process.returncode == 1


def test_bad_connection_bad_host(auth, monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVER_HOST", "bad_host")
    process = subprocess.run(auth)

    assert process.returncode == 1


def test_bad_connection_bad_http_path(auth, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "bad_http_path")
    process = subprocess.run(auth)

    assert process.returncode == 1
