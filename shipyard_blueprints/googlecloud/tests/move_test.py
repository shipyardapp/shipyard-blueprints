import os
import subprocess

import faker
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_templates import CloudStorage

from shipyard_googlecloud import utils

RUN_COMMAND = ["python3", "shipyard_googlecloud/cli/move.py"]
faker = faker.Faker()


@pytest.fixture(autouse=True, scope="session")
def setup():
    load_dotenv(find_dotenv(raise_error_if_not_found=True))
    creds = utils._get_credentials()
    storage_client = utils.get_gclient(creds)

    bucket = storage_client.bucket("shipyard_demo_bucket")

    # Define test files and their initial and final locations
    test_files = [
        "test/move_test.txt",
        "test/move_test2.txt",
        "test/move_nested_file.txt",
        "test/move_nested_file_1.txt",
    ]

    # Upload test files to the 'test' folder in the bucket
    for test_file in test_files:
        blob = bucket.blob(test_file)
        blob.upload_from_string(faker.paragraph(1000))

    yield

    # Cleanup: Remove test files from both 'test' and 'test/move' folders
    for test_file in test_files:
        original_blob = bucket.blob(test_file)
        moved_blob = bucket.blob(f"test/move/{os.path.basename(test_file)}")
        if original_blob.exists():
            original_blob.delete()
        if moved_blob.exists():
            moved_blob.delete()


def _command(
    source_folder_name="",
    destination_folder_name="test/move",
    source_file_name_match_type="",
    source_file_name="",
    service_account="",
):
    return [
        *RUN_COMMAND,
        "--source-bucket-name",
        "shipyard_demo_bucket",
        "--destination-bucket-name",
        "shipyard_demo_bucket",
        "--source-file-name-match-type",
        source_file_name_match_type,
        "--source-file-name",
        source_file_name,
        "--source-folder-name",
        source_folder_name,
        "--destination-folder-name",
        destination_folder_name,
        "--service-account",
        service_account,
    ]


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_exact_match():
    test_files = [
        "move_test.txt",
        "move_test2.txt",
        "move_nested_file.txt",
        "move_nested_file_1.txt",
    ]
    for test_file in test_files:
        test = subprocess.run(
            _command(
                source_folder_name="test",
                source_file_name_match_type="exact_match",
                source_file_name=test_file,
                service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert (
            test.returncode == 0
        ), f"Expected 0, got {test.returncode} for {test_file}"


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_regex_match():
    test = subprocess.run(
        _command(
            source_folder_name="test",
            source_file_name_match_type="regex_match",
            source_file_name=".*move.*\\.txt$",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"


# Add more tests for bad connection and missing files as needed
