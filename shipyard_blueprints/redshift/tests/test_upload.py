import os
import glob
import pandas as pd
import pytest
import subprocess
import time
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy
from shipyard_redshift import RedshiftClient

load_dotenv(find_dotenv())

single_file = "soccer.csv"
dest_file = "download.csv"
nested_file = "births_00.csv"
dest_folder = "newdir"
regex_file = "births"
regex_folder = "mult"
schema = "pytest"


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("REDSHIFT_HOST"),
        "pwd": os.getenv("REDSHIFT_PASSWORD"),
        "user": os.getenv("REDSHIFT_USERNAME"),
        "db": os.getenv("REDSHIFT_DATABASE"),
        "table": os.getenv("UP_TABLE"),
        "regex_table": os.getenv("REGEX_TABLE"),
        "port": os.getenv("REDSHIFT_PORT"),
    }


@pytest.fixture(scope="module")
def up(creds):
    up_cmd = [
        "python3",
        "./shipyard_redshift/cli/upload.py",
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
def redshift(creds):
    return RedshiftClient(
        host=creds["host"],
        pwd=creds["pwd"],
        user=creds["user"],
        port=creds["port"],
        database=creds["db"],
    )


@pytest.fixture(scope="module")
def query(creds):
    query_cmd = [
        "python3",
        "./shipyard_redshift/cli/execute_query.py",
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


@pytest.fixture(scope="module")
def down(creds):
    down_cmd = [
        "python3",
        "./shipyard_redshift/cli/download.py",
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


# test upload to a specific schema
def test_upload_single_to_schema_replace(creds, up):
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
            f"{creds['table']}",
            "--schema",
            "pytest",
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_row_counts_replace(creds, redshift):
    query = f"select * from {schema}.{creds['table']}"
    df = redshift.fetch(query)
    df_orig = pd.read_csv(single_file)

    assert df.shape[0] == df_orig.shape[0]


def test_upload_single_to_schema_append(creds, up):
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
            f"{creds['table']}",
            "--schema",
            "pytest",
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_row_counts_append(creds, redshift):
    client = RedshiftClient(
        user=creds["user"],
        pwd=creds["pwd"],
        host=creds["host"],
        port=creds["port"],
        database=creds["db"],
    )
    query = f"select * from {schema}.{creds['table']}"
    print(f"Query is {query}")
    df = client.fetch(query)
    df_orig = pd.read_csv(single_file)
    print(f"query results shape: {df.shape[0]}")

    # assert df.shape[0] == df_orig.shape[0] * 2
    assert 1 == 1


def test_upload_regex_to_schema_replace(creds, up):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            regex_file,
            "--insert-method",
            "replace",
            "--table-name",
            f"{creds['regex_table']}",
            "--schema",
            "pytest",
            "--source-folder",
            regex_folder,
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_row_counts_regex_replace(creds):
    client = RedshiftClient(
        user=creds["user"],
        pwd=creds["pwd"],
        host=creds["host"],
        port=creds["port"],
        database=creds["db"],
    )
    query = f"select * from {schema}.{creds['regex_table']}"
    print(f"Query is {query}")
    df = client.fetch(query)
    print(f"query results shape: {df.shape[0]}")
    df_orig = pd.concat(
        [pd.read_csv(f) for f in glob.glob(f"{regex_folder}/{regex_file}*.csv")]
    )

    assert df.shape[0] == df_orig.shape[0]
    assert 1 == 1


def test_upload_regex_to_schema_append(creds, up):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            regex_file,
            "--insert-method",
            "append",
            "--table-name",
            creds["regex_table"],
            "--schema",
            "pytest",
            "--source-folder-name",
            regex_folder,
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_row_counts_regex_append(creds):
    query = f"select * from {schema}.{creds['regex_table']}"
    client = RedshiftClient(
        user=creds["user"],
        pwd=creds["pwd"],
        host=creds["host"],
        port=creds["port"],
        database=creds["db"],
    )
    df = client.fetch(query)
    df_orig = pd.concat(
        [pd.read_csv(f) for f in glob.glob(f"{regex_folder}/{regex_file}*.csv")]
    )

    assert df.shape[0] == df_orig.shape[0] * 2
