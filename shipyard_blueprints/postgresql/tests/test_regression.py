import os
import glob
import pandas as pd
import pytest
import subprocess
from shipyard_postgresql import PostgresqlClient
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())

single_file = "test.csv"
dest_file = "download.csv"
nested_file = "mult_1.csv"
dest_folder = "newdir"
regex_file = "mult"
regex_folder = "mult"


def read_all_csvs(dir: str) -> pd.DataFrame:
    pat = f"{dir}/*.csv"
    files = glob.glob(pat)
    dfs = []
    for f in files:
        df = pd.read_csv(f)
        dfs.append(df)

    frame = pd.concat(dfs, axis=0, ignore_index=True)

    return frame


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("POSTGRES_HOST"),
        "pwd": os.getenv("POSTGRES_PASSWORD"),
        "user": os.getenv("POSTGRES_USERNAME"),
        "db": os.getenv("POSTGRES_DATABASE"),
        "table": os.getenv("UP_TABLE"),
        "regex_table": os.getenv("REGEX_TABLE"),
        "port": os.getenv("POSTGRES_PORT"),
    }


@pytest.fixture(scope="module")
def up(creds):
    up_cmd = [
        "python3",
        "./shipyard_postgresql/cli/upload_file.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
        "--port",
        creds["port"],
    ]
    return up_cmd


@pytest.fixture(scope="module")
def down(creds):
    down_cmd = [
        "python3",
        "./shipyard_postgresql/cli/store_query_results.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
        "--port",
        creds["port"],
    ]
    return down_cmd


@pytest.fixture(scope="module")
def query(creds):
    query_cmd = [
        "python3",
        "./shipyard_postgresql/cli/execute_sql.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
        "--port",
        creds["port"],
    ]
    return query_cmd


# End to End 1: Single file exact match, replace
def test_upload_exact_match(creds: dict[str, str], up: list):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            single_file,
            "--insert-method",
            "replace",
            "--table-name",
            creds["table"],
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_download_table(creds: dict[str, str], down: list):
    down_copy = deepcopy(down)
    down_copy.extend(
        [
            "--query",
            f'select * from {creds["table"]}',
            "--destination-file-name",
            dest_file,
        ]
    )

    process = subprocess.run(down_copy)

    assert process.returncode == 0


def test_row_counts(creds):
    orig = pd.read_csv(single_file)
    new = pd.read_csv(dest_file)
    assert orig.shape[0] == new.shape[0]
    print("Removing downloaded file")
    os.remove(dest_file)


def test_upload_exact_match_append(creds: dict[str, str], up: list):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            single_file,
            "--insert-method",
            "append",
            "--table-name",
            creds["table"],
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_download_table_append(creds: dict[str, str], down: list):
    down_copy = deepcopy(down)
    down_copy.extend(
        [
            "--query",
            f'select * from {creds["table"]}',
            "--destination-file-name",
            dest_file,
        ]
    )

    process = subprocess.run(down_copy)

    assert process.returncode == 0


def test_row_counts_append(creds):
    orig = pd.read_csv(single_file)
    new = pd.read_csv(dest_file)
    assert (2 * orig.shape[0]) == new.shape[0]
    print("Removing downloaded file")
    os.remove(dest_file)


def test_drop_table_1(creds, query):
    query_copy = deepcopy(query)
    query_copy.extend(["--query", f'drop table {creds["table"]}'])

    process = subprocess.run(query_copy)

    assert process.returncode == 0
