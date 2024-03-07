import pytest
import os
import subprocess
import pandas as pd
import glob
from copy import deepcopy
from dotenv import load_dotenv, find_dotenv


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
        "host": os.getenv("SQL_HOST"),
        "pwd": os.getenv("SQL_PWD"),
        "user": os.getenv("SQL_USER"),
        "db": os.getenv("SQL_DB"),
        "table": os.getenv("UP_TABLE"),
        "regex_table": os.getenv("REGEX_TABLE"),
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
def down(creds):
    down_cmd = [
        "python3",
        "./shipyard_sqlserver/cli/store_query_results.py",
        "--username",
        creds["user"],
        "--password",
        creds["pwd"],
        "--host",
        creds["host"],
        "--database",
        creds["db"],
    ]
    return down_cmd


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


# BUG: this is failing, the source table has over 70k records, whereas it should have 10k
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


# BUG: This is failing, the source table has over 90k rows
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


# BUG: This is failing, the source table has over 90k rows
def test_rows_regex():
    orig = read_all_csvs(regex_folder)
    new = pd.read_csv(dest_file)
    assert (orig.shape[0]) == new.shape[0]
    print("Removing downloaded file")
    os.remove(dest_file)


# Clean Up


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
