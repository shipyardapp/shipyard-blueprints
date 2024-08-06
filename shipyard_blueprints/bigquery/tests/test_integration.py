import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(find_dotenv())


@pytest.fixture
def upload():
    return [
        "python3",
        "./shipyard_bigquery/cli/upload.py",
        "--service-account",
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    ]


@pytest.fixture
def download():
    return [
        "python3",
        "./shipyard_bigquery/cli/download.py",
        "--service-account",
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    ]


@pytest.fixture
def execute_query():
    return [
        "python3",
        "./shipyard_bigquery/cli/execute_query.py",
        "--service-account",
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    ]


# upload the file to BigQuery
def test_upload_replace(upload):
    cmd = deepcopy(upload)
    cmd.extend(
        [
            "--upload-type",
            "overwrite",
            "--source-file-name",
            "single.csv",
            "--dataset",
            "blueprint_testing",
            "--table",
            "pytest_upload_test",
        ]
    )
    process = subprocess.run(cmd, check=True)

    assert process.returncode == 0


# download the query results from BigQuery
def test_download_replaced_table(download):
    cmd = deepcopy(download)

    cmd.extend(
        [
            "--query",
            "select * from blueprint_testing.pytest_upload_test",
            "--destination-file-name",
            "pytest_download_test.csv",
        ]
    )
    process = subprocess.run(cmd, check=True)
    assert process.returncode == 0


# check that the file exists and was properly downloaded
def test_file_exists():
    assert os.path.exists("pytest_download_test.csv")
    os.remove("pytest_download_test.csv")


# drop the table
def test_drop_table(execute_query):
    cmd = deepcopy(execute_query)
    cmd.extend(
        [
            "--query",
            "DROP TABLE blueprint_testing.pytest_upload_test",
        ]
    )

    process = subprocess.run(cmd, check=True)
    assert process.returncode == 0
