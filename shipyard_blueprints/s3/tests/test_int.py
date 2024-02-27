import os
import subprocess
import pytest
import shutil
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def aws_credentials():
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "REGION": os.getenv("REGION"),
        "BUCKET": os.getenv("BUCKET"),
        "S3_FOLDER": os.getenv("S3_FOLDER"),
    }


def test_upload_download_integration(aws_credentials):
    if not all(aws_credentials.values()):
        pytest.skip("AWS credentials are not properly configured.")

    # Upload
    upload_command = [
        "python3",
        "./shipyard_s3/cli/upload.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--source-file-name-match-type",
        "exact_match",
        "--source-file-name",
        "s3.csv",
        "--destination-folder-name",
        aws_credentials["S3_FOLDER"],
    ]
    subprocess.run(upload_command, check=True)

    # Download
    download_command = [
        "python3",
        "./shipyard_s3/cli/download.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
        "--source-file-name",
        "s3.csv",
        "--source-file-name-match-type",
        "exact_match",
        "--destination-file-name",
        "new_s3.csv",
    ]
    subprocess.run(download_command, check=True)

    # Check if files match
    original_rows = sum(1 for _ in open("s3.csv"))
    new_rows = sum(1 for _ in open("new_s3.csv"))

    assert original_rows == new_rows, "Files do not match"

    # Cleanup
    os.remove("new_s3.csv")


def test_rename_remove_integration(aws_credentials):
    rename_command = [
        "python3",
        "./shipyard_s3/cli/move.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--source-bucket-name",
        aws_credentials["BUCKET"],
        "--destination-bucket-name",
        aws_credentials["BUCKET"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--source-file-name-match-type",
        "exact_match",
        "--source-file-name",
        "s3.csv",
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
        "--destination-folder-name",
        aws_credentials["S3_FOLDER"],
        "--destination-file-name",
        "new_s3.csv",
    ]
    subprocess.run(rename_command, check=True)

    remove_command = [
        "python3",
        "./shipyard_s3/cli/remove.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--source-file-name-match-type",
        "exact_match",
        "--source-file-name",
        "new_s3.csv",
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
    ]

    subprocess.run(remove_command, check=True)


def test_upload_from_folder_download_to_folder(aws_credentials):
    upload_command = [
        "python3",
        "./shipyard_s3/cli/upload.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--source-file-name-match-type",
        "regex_match",
        "--source-file-name",
        "data",
        "--source-folder-name",
        "test_folder",
        "--destination-folder-name",
        aws_credentials["S3_FOLDER"],
        "--destination-file-name",
        "regex.csv",
    ]
    subprocess.run(upload_command, check=True)

    download_command = [
        "python3",
        "./shipyard_s3/cli/download.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
        "--source-file-name",
        "regex",
        "--source-file-name-match-type",
        "regex_match",
        "--destination-folder-name",
        "reg_download",
    ]
    subprocess.run(download_command, check=True)
    shutil.rmtree("reg_download")


def test_regex_rename_and_regex_delete(aws_credentials):
    rename_command = [
        "python3",
        "./shipyard_s3/cli/move.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--source-bucket-name",
        aws_credentials["BUCKET"],
        "--destination-bucket-name",
        aws_credentials["BUCKET"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--source-file-name-match-type",
        "regex_match",
        "--source-file-name",
        "regex",
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
        "--destination-folder-name",
        aws_credentials["S3_FOLDER"],
        "--destination-file-name",
        "delete_me",
    ]
    subprocess.run(rename_command, check=True)

    remove_command = [
        "python3",
        "./shipyard_s3/cli/remove.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--source-file-name-match-type",
        "regex_match",
        "--source-file-name",
        "delete_me",
        "--source-folder-name",
        aws_credentials["S3_FOLDER"],
    ]

    subprocess.run(remove_command, check=True)


def test_upload_without_dest_folder(aws_credentials):
    upload_command = [
        "python3",
        "./shipyard_s3/cli/upload.py",
        "--aws-access-key-id",
        aws_credentials["AWS_ACCESS_KEY_ID"],
        "--aws-secret-access-key",
        aws_credentials["AWS_SECRET_ACCESS_KEY"],
        "--aws-default-region",
        aws_credentials["REGION"],
        "--bucket-name",
        aws_credentials["BUCKET"],
        "--source-file-name-match-type",
        "exact_match",
        "--source-file-name",
        "s3.csv",
        "--destination-file-name",
        "pytest_upload.csv",
    ]
    subprocess.run(upload_command, check=True)
