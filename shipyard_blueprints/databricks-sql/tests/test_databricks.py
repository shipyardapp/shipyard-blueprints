import pytest
import os
import subprocess
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from shipyard_databricks_sql import DatabricksSqlClient


load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def credentials():
    return {
        "server": os.getenv("DATABRICKS_SERVER_HOST"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "token": os.getenv("DATABRICKS_SQL_ACCESS_TOKEN"),
    }


@pytest.fixture(scope="module")
def vars():
    return {
        "table1": "pytest_upload",
    }


def create_file(name: str, file_type: str = "csv", n: int = 1, rows: int = 1000):
    cmd = ["datagen", "create", "-f", name, "-y", file_type, "-r", rows, "-m", n]
    subprocess.run(cmd)


def test_upload_replace(credentials, vars):
    up_cmd = [
        "python3",
        "./shipyard_databricks_sql/cli/upload.py",
        "--access-token",
        credentials["token"],
        "--server-host",
        credentials["server"],
        "--http-path",
        credentials["http_path"],
        "--catalog",
        os.getenv("DEMO_CATALOG"),
        "--schema",
        os.getenv("DEMO_SCHEMA"),
        "--table-name",
        vars["table1"],
        "--volume",
        os.getenv("DEMO_VOLUME"),
        "--insert-method",
        "replace",
        "--file-type",
        "csv",
        "--file-name",
        "up_replace.csv",
        "--match-type",
        "exact_match",
    ]

    up_result = subprocess.run(up_cmd)

    # check that it ran successfully
    assert up_result.returncode == 0

    target_file = "result.csv"
    down_cmd = [
        "python3",
        "./shipyard_databricks_sql/cli/fetch.py",
        "--access-token",
        credentials["token"],
        "--server-host",
        credentials["server"],
        "--http-path",
        credentials["http_path"],
        "--catalog",
        os.getenv("DEMO_CATALOG"),
        "--schema",
        os.getenv("DEMO_SCHEMA"),
        "--query",
        f'select * from {vars["table1"]}',
        "--file-type",
        "csv",
        "--file-name",
        target_file,
    ]

    down_result = subprocess.run(down_cmd)
    # check that it ran successfully
    assert down_result.returncode == 0

    # check that the row counts are equal
    df = pd.read_csv(target_file)
    orig_df = pd.read_csv("up_replace.csv")

    new_row_counts = df.shape[0]
    orig_row_counts = orig_df.shape[0]

    assert new_row_counts == orig_row_counts

    os.remove(target_file)


def test_upload_append(credentials, vars):
    up_cmd = [
        "python3",
        "./shipyard_databricks_sql/cli/upload.py",
        "--access-token",
        credentials["token"],
        "--server-host",
        credentials["server"],
        "--http-path",
        credentials["http_path"],
        "--catalog",
        os.getenv("DEMO_CATALOG"),
        "--schema",
        os.getenv("DEMO_SCHEMA"),
        "--table-name",
        vars["table1"],
        "--volume",
        os.getenv("DEMO_VOLUME"),
        "--insert-method",
        "append",
        "--file-type",
        "csv",
        "--file-name",
        "up_replace.csv",
        "--match-type",
        "exact_match",
    ]
    up_result = subprocess.run(up_cmd)

    # check that it ran successfully
    assert up_result.returncode == 0


def test_download_append_job(credentials, vars):
    target_file = "result_append.csv"
    down_cmd = [
        "python3",
        "./shipyard_databricks_sql/cli/fetch.py",
        "--access-token",
        credentials["token"],
        "--server-host",
        credentials["server"],
        "--http-path",
        credentials["http_path"],
        "--catalog",
        os.getenv("DEMO_CATALOG"),
        "--schema",
        os.getenv("DEMO_SCHEMA"),
        "--query",
        f'select * from {vars["table1"]}',
        "--file-type",
        "csv",
        "--file-name",
        target_file,
    ]

    down_result = subprocess.run(down_cmd)
    # check that it ran successfully
    assert down_result.returncode == 0

    # check that the row counts are equal
    df = pd.read_csv(target_file)
    orig_df = pd.read_csv("up_replace.csv")

    assert df.shape[0] == 2 * (orig_df.shape[0])


def test_drop_table(credentials, vars):
    exec_cmd = [
        "python3",
        "./shipyard_databricks_sql/cli/execute.py",
        "--access-token",
        credentials["token"],
        "--server-host",
        credentials["server"],
        "--http-path",
        credentials["http_path"],
        "--catalog",
        os.getenv("DEMO_CATALOG"),
        "--schema",
        os.getenv("DEMO_SCHEMA"),
        "--query",
        f'drop table if exists {vars["table1"]}',
    ]
    res = subprocess.run(exec_cmd)

    assert res.returncode == 0
