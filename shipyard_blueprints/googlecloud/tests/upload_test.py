import os
import subprocess

import faker
import pytest

from dotenv import load_dotenv, find_dotenv
from shipyard_templates import CloudStorage

RUN_COMMAND = ["python3", "shipyard_googlecloud/cli/upload.py"]
faker = faker.Faker()


@pytest.fixture(autouse=True, scope="session")
def setup():
    load_dotenv(find_dotenv(raise_error_if_not_found=True))
    with open("test.txt", "w") as f:
        f.write(faker.text())

    with open("test2.txt", "w") as f:
        f.write(faker.paragraph(100))

    with open("tests/nested_file.txt", "w") as f:
        f.write(faker.paragraph(100))
    with open("tests/nested_file_1.txt", "w") as f:
        f.write(faker.paragraph(1000))
    yield
    os.remove("test.txt")
    os.remove("test2.txt")
    os.remove("tests/nested_file.txt")
    os.remove("tests/nested_file_1.txt")


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
def test_exact_match_uploads():
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="test.txt",
            destination_folder_name="pytest",
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
            source_file_name="test2.txt",
            destination_folder_name="pytest",
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
            source_file_name="nested_file.txt",
            source_folder_name="tests",
            destination_folder_name="pytest",
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
            source_file_name="nested_file_1.txt",
            source_folder_name="tests",
            destination_folder_name="pytest",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert test.returncode == 0, f"Expected 0, got {test.returncode}"


@pytest.mark.skipif(not load_dotenv(find_dotenv()), reason="No .env file found")
def test_regex_match_uploads():
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="regex_match",
            source_file_name="test.*",
            destination_folder_name="pytest",
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
            destination_folder_name="pytest",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert test.returncode == 0, f"Expected 0, got {test.returncode}"


def test_missing_files():
    expected_code = CloudStorage.EXIT_CODE_FILE_NOT_FOUND
    test = subprocess.run(
        _command(
            bucket_name="shipyard_demo_bucket",
            source_file_name_match_type="exact_match",
            source_file_name="doesnotexist.txt",
            destination_folder_name="pytest",
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
            source_file_name="test3.*",
            destination_folder_name="pytest",
            service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        test.returncode == expected_code
    ), f"Expected {expected_code}, got {test.returncode}"
