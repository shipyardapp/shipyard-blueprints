import pytest
import subprocess
from copy import deepcopy


@pytest.fixture(scope="module")
def convert():
    return ["python3", "./shipyard_file_manipulation/cli/convert.py"]


def compress():
    return ["python3", "./shipyard_file_manipulation/compress.py"]


def decompress():
    return ["python3", "./shipyard_file_manipulation/decompress.py"]


def test_convert(convert):
    cmd = deepcopy(convert)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
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
