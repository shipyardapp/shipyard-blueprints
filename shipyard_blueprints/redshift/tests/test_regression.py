import os
import glob
import pandas as pd
import pytest
import subprocess
from shipyard_redshift import RedshiftClient
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())

single_file = "soccer.csv"
dest_file = "download.csv"
nested_file = "births_00.csv"
dest_folder = "newdir"
regex_file = "births"
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


# End to End 2: Single file exact match, append


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


# End to End 3: Single file within a folder, exact match, replace


def test_upload_nested_file_replace(creds: dict[str, str], up: list):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            nested_file,
            "--source-folder-name",
            "mult",
            "--insert-method",
            "replace",
            "--table-name",
            creds["table"],
        ]
    )

    process = subprocess.run(up_copy)

    assert process.returncode == 0


def test_download_table_nested(creds: dict[str, str], down: list):
    down_copy = deepcopy(down)
    down_copy.extend(
        [
            "--query",
            f'select * from {creds["table"]}',
            "--destination-file-name",
            dest_file,
            "--destination-folder-name",
            dest_folder,
        ]
    )

    process = subprocess.run(down_copy)

    assert process.returncode == 0


def test_row_counts_nested(creds):
    orig = pd.read_csv(f"mult/{nested_file}")
    new = pd.read_csv(f"{dest_folder}/{dest_file}")
    assert (orig.shape[0]) == new.shape[0]
    print("Removing downloaded file and directory")
    subprocess.run(["rm", "-fr", dest_folder])


# End to End 4: Upload many files within a folder
def test_upload_regex_match_replace(creds, up):
    up_copy = deepcopy(up)
    up_copy.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            regex_file,
            "--source-folder-name",
            regex_folder,
            "--insert-method",
            "replace",
            "--table-name",
            creds["regex_table"],
        ]
    )

    process = subprocess.run(up_copy)
    assert process.returncode == 0


def test_download_regex_match_table(creds, down):
    down_copy = deepcopy(down)
    down_copy.extend(
        [
            "--query",
            f'select * from {creds["regex_table"]}',
            "--destination-file-name",
            dest_file,
        ]
    )

    process = subprocess.run(down_copy)

    assert process.returncode == 0


# BUG: This is failing in the original version, post refactor should pass
def test_rows_regex():
    orig = read_all_csvs(regex_folder)
    new = pd.read_csv(dest_file)
    print("Removing downloaded file")
    os.remove(dest_file)
    assert (orig.shape[0]) == new.shape[0]


def test_drop_table_1(creds, query):
    query_copy = deepcopy(query)
    query_copy.extend(["--query", f'drop table {creds["table"]}'])

    process = subprocess.run(query_copy)

    assert process.returncode == 0


def test_drop_table_regex(creds, query):
    query_copy = deepcopy(query)
    query_copy.extend(["--query", f'drop table {creds["regex_table"]}'])

    process = subprocess.run(query_copy)

    assert process.returncode == 0
