import pytest
import subprocess
import os
from copy import deepcopy
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

test_file = "test.csv"
dest_folder = "test_compress"
reg_folder = "mult"
reg_dest = "mult_compressed"


@pytest.fixture(scope="module")
def compress():
    return ["python3", "./shipyard_file_manipulation/cli/compress.py"]


def test_compress_zip(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_file,
            "--destination-file-name",
            "test.zip",
            "--compression",
            "zip",
            "--destination-folder-name",
            dest_folder,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_tar(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_file,
            "--destination-file-name",
            "test.tar",
            "--compression",
            "tar",
            "--destination-folder-name",
            dest_folder,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_tarbz2(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_file,
            "--destination-file-name",
            "test.tar.bz2",
            "--compression",
            "tar.bz2",
            "--destination-folder-name",
            dest_folder,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_targz(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "exact_match",
            "--source-file-name",
            test_file,
            "--destination-file-name",
            "test.tar.gz",
            "--compression",
            "tar.gz",
            "--destination-folder-name",
            dest_folder,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def check_files():
    assert len(os.listdir(dest_folder)) == 4


# check regex match functionality
# BUG: regex match is not working the old way when the source folder name is provided
def test_compress_regex_zip(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--source-folder-name",
            reg_folder,
            "--destination-file-name",
            "mult.zip",
            "--compression",
            "zip",
            "--destination-folder-name",
            reg_dest,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_regex_tar(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--source-folder-name",
            reg_folder,
            "--destination-file-name",
            "mult.tar",
            "--compression",
            "tar",
            "--destination-folder-name",
            reg_dest,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_regex_tarbz2(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--source-folder-name",
            reg_folder,
            "--destination-file-name",
            "mult.tar.bz2",
            "--compression",
            "tar.bz2",
            "--destination-folder-name",
            reg_dest,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def test_compress_regex_targz(compress):
    cmd = deepcopy(compress)
    cmd.extend(
        [
            "--source-file-name-match-type",
            "regex_match",
            "--source-file-name",
            "mult",
            "--source-folder-name",
            reg_folder,
            "--destination-file-name",
            "mult.tar.gz",
            "--compression",
            "tar.gz",
            "--destination-folder-name",
            reg_dest,
        ]
    )
    process = subprocess.run(cmd)
    assert process.returncode == 0


def check_regex_files():
    assert len(os.listdir(reg_dest)) == 4


# def cleanup():
#     cmd = ["rm", "-rf", dest_folder]
#     process = subprocess.run(cmd)
#     assert process.returncode == 0
#     cmd2 = ["rm", "-rf", reg_dest]
#     process2 = subprocess.run(cmd2)
#     assert process2.returncode == 0
