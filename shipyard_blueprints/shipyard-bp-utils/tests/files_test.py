from shipyard_bp_utils import files

import pytest
import os


@pytest.mark.parametrize(
    "destination_file_name,file_number,expected_result",
    [
        ("output.txt", None, "output.txt"),
        ("output.txt", 1, "output_1.txt"),
        ("", None, "source.txt"),
        ("", 2, "source_2.txt"),
        ("output", None, "output"),
        ("output", 3, "output_3"),
    ],
)
def test_determine_destination_file_name(
    destination_file_name, file_number, expected_result
):
    source_full_path = "/parent/child/source.txt"
    result = files.determine_destination_file_name(
        source_full_path=source_full_path,
        destination_file_name=destination_file_name,
        file_number=file_number,
    )
    assert result == expected_result, f"Expected {expected_result}, got {result}"


def test_determine_destination_file_name_with_empty_source_basename():
    source_full_path = "/random/source/"
    destination_file_name = ""
    file_number = None
    expected_result = ""
    result = files.determine_destination_file_name(
        source_full_path=source_full_path,
        destination_file_name=destination_file_name,
        file_number=file_number,
    )
    assert result == expected_result, f"Expected an empty string, got {result}"


def test_source_full_path_current_dir():
    source_full_path = "source.txt"
    destination_file_name = ""
    file_number = None
    expected_result = "source.txt"
    result = files.determine_destination_file_name(
        source_full_path=source_full_path,
        destination_file_name=destination_file_name,
        file_number=file_number,
    )
    assert result == expected_result, f"Expected {expected_result}, got {result}"


def test_source_full_path_current_dir_with_file_num():
    source_full_path = "source.txt"
    destination_file_name = ""
    file_number = 2
    expected_result = "source_2.txt"
    result = files.determine_destination_file_name(
        source_full_path=source_full_path,
        destination_file_name=destination_file_name,
        file_number=file_number,
    )
    assert result == expected_result, f"Expected {expected_result}, got {result}"


def test_find_all_file_matches_in_folder():
    src_dir = "Upload_Test"
    regex = "Test"
    ans = 2  # the number of matches should be 2
    local_files = files.find_all_local_file_names(source_folder_name=src_dir)

    result = files.find_all_file_matches(local_files, regex)

    assert len(result) == ans


def test_find_all_file_matches_in_cwd():
    regex = "py"
    ans = 3
    local_files = files.find_all_local_file_names()
    result = files.find_all_file_matches(local_files, regex)
    assert len(result) == ans
