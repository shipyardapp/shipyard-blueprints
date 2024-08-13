import os
import subprocess

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_googlecloud import utils

RUN_COMMAND = ["python3", "shipyard_googlecloud/cli/remove.py"]


@pytest.fixture(autouse=True, scope="session")
def setup():
    load_dotenv(find_dotenv(raise_error_if_not_found=True))
    creds = utils._get_credentials()
    storage_client = utils.get_gclient(creds)

    bucket = storage_client.bucket("shipyard_demo_bucket")

    # Define test files to be uploaded
    test_files = [
        "tests/delete_test.txt",
        "tests/delete_test2.txt",
        "tests/nested/delete_nested_file.txt",
        "tests/nested/delete_nested_file_1.txt",
    ]

    # Upload test files to the bucket
    for test_file in test_files:
        blob = bucket.blob(test_file)
        blob.upload_from_string("This is test content.")

    yield

    for test_file in test_files:
        blob = bucket.blob(test_file)
        if blob.exists():
            blob.delete()


def _command(
    bucket_name="shipyard_demo_bucket",
    source_file_name_match_type="",
    source_file_name="",
    source_folder_name="",
    service_account="",
):
    return [
        *RUN_COMMAND,
        "--bucket-name",
        bucket_name,
        "--source-file-name-match-type",
        source_file_name_match_type,
        "--source-file-name",
        source_file_name,
        "--source-folder-name",
        source_folder_name,
        "--service-account",
        service_account,
    ]


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_delete():
    test = subprocess.run(
        _command(
            source_file_name_match_type="exact_match",
            source_file_name="delete_test.txt",
            source_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"

    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="regex_match",
            source_file_name="delete_.*\\.txt$",
            source_folder_name="test",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"
