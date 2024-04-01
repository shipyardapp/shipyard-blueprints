import os
import subprocess

import faker
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_templates import CloudStorage

from shipyard_googlecloud import utils

RUN_COMMAND = ["python3", "shipyard_googlecloud/cli/download.py"]
faker = faker.Faker()


@pytest.fixture(autouse=True, scope="session")
def setup():
    load_dotenv(find_dotenv(raise_error_if_not_found=True))
    tmp_file = utils.set_environment_variables(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    )
    storage_client = utils.get_gclient(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    bucket = storage_client.bucket("shipyard_demo_bucket")

    test_files = [
        "download_test.txt",
        "download_test2.txt",
        "tests/download_nested_file.txt",
        "tests/download_nested_file_1.txt",
    ]

    for test_file in test_files:
        folder_structure = os.path.dirname(test_file)
        if folder_structure and not os.path.exists(folder_structure):
            os.makedirs(folder_structure)

        with open(test_file, "w") as f:
            f.write(faker.paragraph(1000))

        blob = bucket.blob(test_file)
        print("Uploading", test_file, "to", blob.name)
        blob.upload_from_filename(test_file)
    yield

    for test_file in test_files:
        os.remove(test_file)
        blob = bucket.blob(test_file)
        blob.delete()
    # Delete the test download files that should be downloaded during regex testing
    files_to_remove = [
        "tests/download_test.txt",
        "tests/download_test2.txt",
        "tests/download_test2_3.txt",
        "tests/download_nested_file_1_4.txt",
        "tests/download_nested_file_3.txt",
        "tests/download_test2_2.txt",
        "tests/download_test_1.txt",
    ]

    for file_path in files_to_remove:
        if os.path.exists(file_path):
            os.remove(file_path)


def _command(
    bucket_name="",
    source_file_name_match_type="",
    source_file_name="",
    source_folder_name="",
    destination_folder_name="",
    destination_file_name="",
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
        "--destination-folder-name",
        destination_folder_name,
        "--destination-file-name",
        destination_file_name,
        "--service-account",
        service_account,
    ]


# shipyard_demo_bucket


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_exact_match():
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="download_nested_file.txt",
            source_folder_name="tests",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="download_nested_file_1.txt",
            source_folder_name="tests",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"

    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="download_test.txt",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert test.returncode == 0, f"Expected 0, got {test.returncode}."
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="download_test.txt",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert test.returncode == 0, f"Expected 0, got {test.returncode}"


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_regex_match():
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="regex_match",
            source_file_name=".*download.*\\.txt$",
            destination_folder_name="tests",
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
            source_file_name="nested_file.*",
            source_folder_name="tests",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"


def test_bad_connection():
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="regex_match",
            source_file_name="test.*",
            destination_folder_name="tests",
            service_account="bad_creds",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        test.returncode == CloudStorage.EXIT_CODE_INVALID_CREDENTIALS
    ), f"Expected {CloudStorage.EXIT_CODE_INVALID_CREDENTIALS}, got {test.returncode}"


def test_missing_files():
    expected_code = CloudStorage.EXIT_CODE_FILE_NOT_FOUND
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="doesnotexist.txt",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        test.returncode == expected_code
    ), f"Expected {expected_code}, got {test.returncode}"
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="regex_match",
            source_file_name="doesnotexist",
            source_folder_name="tests",
            destination_folder_name="tests",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        test.returncode == expected_code
    ), f"Expected {expected_code}, got {test.returncode}"
