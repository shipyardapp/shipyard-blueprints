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


def test_compress_into_folder(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--destination-folder-name",
            "source_folder",
            "--source-file-name",
            test_csv,
            "--destination-file-name",
            nested_zip,
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


def test_convert_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.parquet",
            "--destination-file-format",
            "parquet",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("converted.parquet")


def test_convert_to_tsv(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.tsv",
            "--destination-file-format",
            "tsv",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("converted.tsv")


def test_convert_to_xlsx(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.xlsx",
            "--destination-file-format",
            "xlsx",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("converted.xlsx")


def test_convert_to_psv(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.psv",
            "--destination-file-format",
            "psv",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("converted.psv")


def test_convert_to_stata(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.dta",
            "--destination-file-format",
            "stata",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("converted.dta")


# BUG: this doesn't work without the Tables package. Going to try to circumvent this by using pandas
def test_convert_to_hdf5(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.h5",
            "--destination-file-format",
            "hdf5",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 1

    # removing the files created during the test
    # os.remove("converted.h5")


# BUG: this fails if the folder doesn't exist
def test_convert_to_parquet_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.parquet",
            "--destination-file-format",
            "parquet",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("test_folder/converted.parquet")


# BUG: this fails if the folder doesn't exist
def test_convert_to_psv_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.psv",
            "--destination-file-format",
            "psv",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("test_folder/converted.psv")


# BUG: this fails if the folder doesn't exist
def test_convert_to_tsv_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.tsv",
            "--destination-file-format",
            "tsv",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("test_folder/converted.tsv")


# BUG: this fails if the folder doesn't exist
def test_convert_to_xlsx_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.xlsx",
            "--destination-file-format",
            "xlsx",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
    os.remove("test_folder/converted.xlsx")


# BUG: this fails if the folder doesn't exist
def test_convert_to_stata_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "converted.dta",
            "--destination-file-format",
            "stata",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test


# BUG: this fails if the folder doesn't exist
def test_compress_with_folder(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "test_folder",
            "--source-file-name",
            "test.csv",
            "--destination-file-name",
            "test.zip",
            "--compression",
            "zip",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test


def test_decompress_with_folder(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            "nested.zip",
            "--source-folder-name",
            "source_folder",
            "--destination-file-name",
            "from_nested",
            "--compression",
            "zip",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0

    # removing the files created during the test
