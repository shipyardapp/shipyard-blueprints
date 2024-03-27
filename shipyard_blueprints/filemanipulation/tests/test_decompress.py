import pytest
import subprocess
import os
from copy import deepcopy


@pytest.fixture(scope="module")
def decompress():
    return ["python3", "./shipyard_file_manipulation/cli/decompress.py"]


src_dir = "test_compress"  # the folder where the compressed files are stored
target_dir = "test_decompress"  # the folder where the decompressed files will be stored


def test_decompress_zip(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            "test.zip",
            "--destination-file-name",
            "zip.csv",
            "--compression",
            "zip",
            "--destination-folder-name",
            target_dir,
            "--source-folder-name",
            src_dir,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_decompress_tar(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            "test.tar",
            "--destination-file-name",
            "tar.csv",
            "--compression",
            "tar",
            "--destination-folder-name",
            target_dir,
            "--source-folder-name",
            src_dir,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_decompress_tarbz2(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            "test.tar.bz2",
            "--destination-file-name",
            "tarbz2.csv",
            "--compression",
            "tar.bz2",
            "--destination-folder-name",
            target_dir,
            "--source-folder-name",
            src_dir,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_decompress_targz(decompress):
    cmd = deepcopy(decompress)
    cmd.extend(
        [
            "--source-file-name",
            "test.tar.gz",
            "--destination-file-name",
            "targz.csv",
            "--compression",
            "tar.gz",
            "--destination-folder-name",
            target_dir,
            "--source-folder-name",
            src_dir,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0
