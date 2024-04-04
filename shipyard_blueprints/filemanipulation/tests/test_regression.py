import pytest
import subprocess
import os
from copy import deepcopy


@pytest.fixture(scope="module")
def convert():
    return ["python3", "./shipyard_file_manipulation/cli/convert.py"]


@pytest.fixture(scope="module")
def compress():
    return ["python3", "./shipyard_file_manipulation/cli/compress.py"]


@pytest.fixture(scope="module")
def decompress():
    return ["python3", "./shipyard_file_manipulation/cli/decompress.py"]


nested_zip = "nested.zip"
source_folder = "source_folder"
test_folder = "test_folder"
test_csv = "test.csv"
test_zip = "test.zip"


# BASE CASE TESTS
def test_convert(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_csv,
            "--destination-file-name",
            "converted.parquet",
            "--destination-file-format",
            "parquet",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_csv,
            "--destination-file-name",
            test_zip,
            "--compression",
            "zip",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_decompress(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            test_zip,
            "--destination-file-name",
            "decompressed.csv",
            "--compression",
            "zip",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    subprocess.run(["rm", "-rf", "decompressed.csv"])
    os.remove("test.zip")
