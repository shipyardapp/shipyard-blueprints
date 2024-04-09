import pytest
import os
import subprocess
import pandas as pd
import glob
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(find_dotenv())

# variables
TABLE1 = "pytest_upload"
GLOB_TABLE = "pytest_glob"
DIR = "mult"
TARGET_GLOB = f"downloaded_{GLOB_TABLE}.csv"


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
def credentials():
    return {
        "server": os.getenv("DATABRICKS_SERVER_HOST"),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
        "token": os.getenv("DATABRICKS_SQL_ACCESS_TOKEN"),
    }


@pytest.fixture(scope="module")
def vars():
    return {"table1": "pytest_upload", "glob_table": "pytest_glob"}


@pytest.fixture(scope="module")
def upload(credentials):
    return [
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
        "--volume",
        os.getenv("DEMO_VOLUME"),
    ]


@pytest.fixture(scope="module")
def download(credentials):
    return [
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
    ]


def test_upload_glob_in_folder_replace(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--table",
            GLOB_TABLE,
            "--match-type",
            "glob_match",
            "--folder-name",
            DIR,
            "--insert-method",
            "replace",
            "--file-name",
            "data_*",
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_download_uploaded_glob_from_folder(download):
    cmd = deepcopy(download)
    cmd.extend(
        ["--query", f"SELECT * FROM {GLOB_TABLE}", "--file-name", "downloaded_glob.csv"]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_row_count_1():
    df = pd.read_csv(TARGET_GLOB)
    df_2 = read_all_csvs(DIR)
    assert df.shape[0] == df_2.shape[0]


def test_upload_glob_in_folder_append(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--table",
            GLOB_TABLE,
            "--match-type",
            "glob_match",
            "--folder-name",
            DIR,
            "--insert-method",
            "append",
            "--file-name",
            "data_*",
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_download_uploaded_glo_from_folder_2(download):
    cmd = deepcopy(download)
    cmd.extend(
        [
            "--query",
            f"SELECT * FROM {GLOB_TABLE}",
            "--file-name",
            "downloaded_glob_append.csv",
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


# BUG: there is a bug in the append method, it is not incrementing but instead replacing
def test_row_count_2():
    df = pd.read_csv("downloaded_glob_append.csv")
    df_2 = read_all_csvs(DIR)
    assert df.shape[0] == df_2.shape[0] * 2
