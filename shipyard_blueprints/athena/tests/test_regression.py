from botocore.credentials import subprocess
import pytest
import os
import pandas as pd
from typing import Dict
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())

BASECASE_FILE = "basecase.csv"


@pytest.fixture(scope="module")
def creds() -> Dict[str, str]:
    return {
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region": os.getenv("REGION"),
        "query": os.getenv("QUERY"),
        "bucket": os.getenv("BUCKET_NAME"),
        "db": os.getenv("DATABASE"),
        "logs": os.getenv("LOG_BUCKET"),
    }


@pytest.fixture(scope="module")
def down_cmd(creds):
    return [
        "python3",
        "./shipyard_athena/cli/download_query.py",
        "--aws-access-key-id",
        creds["access_key"],
        "--aws-secret-access-key",
        creds["secret_key"],
        "--aws-default-region",
        creds["region"],
        "--bucket-name",
        creds["bucket"],
        "--database",
        creds["db"],
        "--log-folder",
        creds["logs"],
    ]


@pytest.fixture(scope="module")
def exec_cmd(creds):
    return [
        "python3",
        "./shipyard_athena/cli/execute_query.py",
        "--aws-access-key-id",
        creds["access_key"],
        "--aws-secret-access-key",
        creds["secret_key"],
        "--aws-default-region",
        creds["region"],
        "--bucket-name",
        creds["bucket"],
        "--database",
        creds["db"],
    ]


def test_download(creds, down_cmd):
    cmd = deepcopy(down_cmd)
    params = ["--query", creds["query"], "--destination-file-name", BASECASE_FILE]

    cmd.extend(params)

    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_basecase_row_count():
    df = pd.read_csv(BASECASE_FILE)
    assert df.shape[0] == 100

    print("Removing file")
    os.remove(BASECASE_FILE)


def test_execute_query(creds, exec_cmd):
    cmd = deepcopy(exec_cmd)
    cmd.extend(["--query", "select 1"])
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_download_with_folder(creds, down_cmd):
    cmd = deepcopy(down_cmd)
    params = [
        "--query",
        creds["query"],
        "--destination-file-name",
        BASECASE_FILE,
        "--destination-folder-name",
        "nested",
    ]

    cmd.extend(params)

    process = subprocess.run(cmd)

    assert process.returncode == 0

    os.remove(f"nested/{BASECASE_FILE}")
    os.removedirs("nested")


def test_download_with_no_log_folder(creds):
    cmd = [
        "python3",
        "./shipyard_athena/cli/download_query.py",
        "--aws-access-key-id",
        creds["access_key"],
        "--aws-secret-access-key",
        creds["secret_key"],
        "--aws-default-region",
        creds["region"],
        "--bucket-name",
        creds["bucket"],
        "--database",
        "shipyardtest",
        "--query",
        "select * from persons where id is not null limit 100;",
        "--destination-file-name",
        "output.csv",
    ]
    process = subprocess.run(cmd)
    assert process.returncode == 0

    os.remove("output.csv")
