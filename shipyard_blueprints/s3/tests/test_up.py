import os
from os.path import join
import subprocess
import pytest
import shutil
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

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


@pytest.fixture(scope="module")
def up_cmd(aws_credentials):
    return [
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
    ]


@pytest.fixture(scope="module")
def down_cmd(aws_credentials):
    return [
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
    ]


def test_upload_csv(aws_credentials):
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
    process = subprocess.run(upload_command, check=True)
    assert process.returncode == 0


def test_download_csv(aws_credentials):
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
    process = subprocess.run(download_command, check=True)
    assert process.returncode == 0


def test_file_exists():
    assert os.path.isfile("new_s3.csv")
    os.remove("new_s3.csv")


def test_upload_regex_to_new_folder_with_rename(aws_credentials, up_cmd):
    cmd = deepcopy(up_cmd)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "data",
            "--destination-folder-name",
            "pytest_folder",
            "--destination-file-name",
            "pytest_file.csv",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_file_from_new_folder_with_rename(aws_credentials, down_cmd):
    cmd = deepcopy(down_cmd)
    cmd.extend(
        [
            "--source-folder-name",
            "pytest_folder",
            "--source-file-name",
            "pytest_file",
            "--source-file-name-match-type",
            "regex_match",
            "--destination-file-name",
            "pytest_file_download.csv",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


# def test_download_file_from_new_folder_with_rename(aws_credentials, down_cmd):
#     cmd = deepcopy(down_cmd)
#     cmd.extend([
#         "--source-folder-name",
#         "pytest_folder",
#         "--source-file-name",
#         "pytest_file.csv",
#         "--source-file-name-match-type",
#         "exact_match",
#         "--destination-file-name",
#         "pytest_file.csv"
#     ])
#     process = subprocess.run(cmd, capture_output=True)
#     assert process.returncode == 0

# def upload_regex_match_no_folder():
#     cmd = deepcopy(up_cmd)
#     cmd.extend([
#         "--source-file-name-match-type",
#         "regex",
#         "--source-file-name",
#         "data",
#         "--destination-file-name",
#         "pytest_file.csv"
#     ])
#     process = subprocess.run(cmd, capture_output=True)
#     assert process.returncode == 0
