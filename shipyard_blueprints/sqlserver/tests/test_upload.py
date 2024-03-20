import pytest
import os
import subprocess
import pandas as pd
import glob
from copy import deepcopy
from dotenv import load_dotenv, find_dotenv

test_file = "test.csv"

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("SQL_HOST"),
        "pwd": os.getenv("SQL_PWD"),
        "user": os.getenv("SQL_USER"),
        "db": os.getenv("SQL_DB"),
        "table": os.getenv("UP_TABLE"),
        "regex_table": os.getenv("REGEX_TABLE"),
        "new_table": os.getenv("NEW_TABLE"),
    }


@pytest.fixture(scope="module")
def up(creds):
    up_cmd = [
        "python3",
        "./shipyard_sqlserver/cli/upload_file.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
    ]
    return up_cmd


@pytest.fixture(scope="module")
def query(creds):
    query_cmd = [
        "python3",
        "./shipyard_sqlserver/cli/execute_sql.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
    ]
    return query_cmd


def test_upload_append_new_table(creds, up):
    """Even though the table does not exist, the table should created when the insert method is append

    Args:
        creds ():
        up ():
    """
    cmd = deepcopy(up)
    cmd.extend(
        [
            "--source-file-name",
            test_file,
            "--insert-method",
            "append",
            "--table-name",
            creds["new_table"],
        ]
    )

    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_drop_table(creds, query):
    query_copy = deepcopy(query)
    query_copy.extend(["--query", f'drop table {creds["new_table"]}'])

    process = subprocess.run(query_copy)

    assert process.returncode == 0
