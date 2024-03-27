import pytest
import subprocess
import os
from copy import deepcopy


@pytest.fixture(scope="module")
def convert():
    return ["python3", "./shipyard_file_manipulation/cli/convert.py"]


dest_folder = "reg_convert"
dest_folder2 = "single_convert"

del_cmd = ["rm", "-rf", dest_folder]

n_files = 15


def test_convert_regex_parquet(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--destination-file-name",
            "converted.parquet",
            "--destination-file-format",
            "parquet",
            "--destination-folder-name",
            dest_folder,
            "--source-folder-name",
            "mult",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_convert_regex_tsv(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--destination-file-name",
            "converted.tsv",
            "--destination-file-format",
            "tsv",
            "--destination-folder-name",
            dest_folder,
            "--source-folder-name",
            "mult",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_convert_regex_xlsx(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--destination-file-name",
            "converted.xlsx",
            "--destination-file-format",
            "xlsx",
            "--destination-folder-name",
            dest_folder,
            "--source-folder-name",
            "mult",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_convert_regex_psv(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--destination-file-name",
            "converted.psv",
            "--destination-file-format",
            "psv",
            "--destination-folder-name",
            dest_folder,
            "--source-folder-name",
            "mult",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_convert_regex_stata(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--destination-file-name",
            "converted.dta",
            "--destination-file-format",
            "stata",
            "--destination-folder-name",
            dest_folder,
            "--source-folder-name",
            "mult",
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_n_files_expected():
    assert len(os.listdir(dest_folder)) == n_files


def test_convert_to_parquet_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "mult",
            "--source-file-name",
            "mult_1.csv",
            "--destination-file-name",
            "mult_converted.parquet",
            "--destination-file-format",
            "parquet",
            "--destination-folder-name",
            dest_folder2,
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_convert_to_tsv_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "mult",
            "--source-file-name",
            "mult_1.csv",
            "--destination-file-name",
            "mult_converted.tsv",
            "--destination-file-format",
            "tsv",
            "--destination-folder-name",
            dest_folder2,
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_convert_to_xlsx_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "mult",
            "--source-file-name",
            "mult_1.csv",
            "--destination-file-name",
            "mult_converted.xlsx",
            "--destination-file-format",
            "xlsx",
            "--destination-folder-name",
            dest_folder2,
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_convert_to_psv_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "mult",
            "--source-file-name",
            "mult_1.csv",
            "--destination-file-name",
            "mult_converted.psv",
            "--destination-file-format",
            "psv",
            "--destination-folder-name",
            dest_folder2,
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


def test_convert_to_stata_with_folder(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-folder-name",
            "mult",
            "--source-file-name",
            "mult_1.csv",
            "--destination-file-name",
            "mult_converted.dta",
            "--destination-file-format",
            "stata",
            "--destination-folder-name",
            dest_folder2,
        ]
    )
    process = subprocess.run(cmd)

    assert process.returncode == 0


# BUG: the files aren't being written to a folder properly
def test_n_files_expected2():
    assert len(os.listdir(dest_folder2)) == 5


# single tests
def test_single_convert_to_tsv(convert):
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


def test_single_convert_to_xlsx(convert):
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


def test_single_convert_to_psv(convert):
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


def test_single_convert_to_stata(convert):
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
def test_single_convert_to_hdf5(convert):
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


def test_remove_files2():
    process = subprocess.run(["rm", "-rf", dest_folder2])
    assert process.returncode == 0


def test_remove_files():
    process = subprocess.run(del_cmd)
    assert process.returncode == 0
