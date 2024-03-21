"""
The success criteria for file matching is based on the documentation here:
https://www.shipyardapp.com/docs/reference/blueprints/blueprint-library/match-type/#regex-match

These tests are designed to ensure that the file matching sticks to the documentation.

"""

import os

import pytest
from pyfakefs.fake_filesystem_unittest import Patcher

from shipyard_bp_utils.files import file_match, fetch_file_paths_from_directory


@pytest.fixture
def test_file_system():
    """
    Creates a fake file system that matches the documentation
    reports/
    ├─ customer_data.csv
    ├─ external/
    │  ├─ geo_data.csv
    ├─ monthly/
    │  ├─ jan_data.csv
    │  ├─ feb_data.csv
    │  ├─ mar_data.csv
    ├─ aggregated/
    │  ├─ data_yearly.csv
    │  ├─ data_quarterly.csv
    │  ├─ data_monthly.csv
    test/
    │  ├─ test.csv

    company_data.csv
    """
    with Patcher() as patcher:
        # Create directories
        patcher.fs.create_dir("/reports/external")
        patcher.fs.create_dir("/reports/monthly")
        patcher.fs.create_dir("/reports/aggregated")
        patcher.fs.create_dir("/test")

        # Create files
        patcher.fs.create_file("/reports/customer_data.csv")
        patcher.fs.create_file("/reports/external/geo_data.csv")
        patcher.fs.create_file("/reports/monthly/jan_data.csv")
        patcher.fs.create_file("/reports/monthly/feb_data.csv")
        patcher.fs.create_file("/reports/monthly/mar_data.csv")
        patcher.fs.create_file("/reports/aggregated/data_yearly.csv")
        patcher.fs.create_file("/reports/aggregated/data_quarterly.csv")
        patcher.fs.create_file("/reports/aggregated/data_monthly.csv")
        patcher.fs.create_file("/company_data.csv")
        patcher.fs.create_file("/test/test.csv")

        yield patcher


def test_exact_match_original_filename(test_file_system):
    source_file_match_type = "exact_match"
    source_file_name = "jan_data.csv"
    source_folder_name = "reports/monthly"
    destination_folder_name = ""
    destination_file_name = ""

    local_files = fetch_file_paths_from_directory(source_folder_name)

    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )

    source = matches[0]["source_path"]
    destination = matches[0]["destination_filename"]
    assert source == "reports/monthly/jan_data.csv", "Source path is incorrect"
    assert destination == "jan_data.csv", "Destination file name is incorrect"

    assert os.path.exists(source)


#
#
def test_exact_match_new_file(test_file_system):
    source_file_match_type = "exact_match"
    source_file_name = "jan_data.csv"
    source_folder_name = "reports/monthly"
    destination_folder_name = "january"
    destination_file_name = "january_2022.csv"
    local_files = fetch_file_paths_from_directory(source_folder_name)
    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )
    source = matches[0]["source_path"]
    destination = matches[0]["destination_filename"]
    assert source == "reports/monthly/jan_data.csv"
    assert destination == "january/january_2022.csv"

    assert os.path.exists(source)


def test_exact_match_no_match(test_file_system):
    source_file_match_type = "exact_match"
    source_file_name = "jan_data.csv"
    source_folder_name = ""
    destination_folder_name = ""
    destination_file_name = ""

    local_files = fetch_file_paths_from_directory(source_folder_name)
    with pytest.raises(FileNotFoundError):
        matches = file_match(
            search_term=source_file_name,
            source_directory=source_folder_name,
            destination_filename=destination_file_name,
            destination_directory=destination_folder_name,
            files=local_files,
            match_type=source_file_match_type,
        )


#
def test_regex_match_fully_filtered_match(test_file_system):
    source_file_match_type = "regex_match"
    source_file_name = "_data\.csv"
    source_folder_name = "reports/monthly"
    destination_folder_name = ""
    destination_file_name = ""
    local_files = fetch_file_paths_from_directory(source_folder_name)
    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )

    assert len(matches) == 3
    expected_results = [
        {
            "source_path": "reports/monthly/jan_data.csv",
            "destination_filename": "jan_data.csv",
        },
        {
            "source_path": "reports/monthly/feb_data.csv",
            "destination_filename": "feb_data.csv",
        },
        {
            "source_path": "reports/monthly/mar_data.csv",
            "destination_filename": "mar_data.csv",
        },
    ]
    assert matches == expected_results


def test_regex_match_subfolder_match(test_file_system):
    source_file_match_type = "regex_match"
    source_file_name = "monthly"
    source_folder_name = "reports"
    destination_folder_name = ""
    destination_file_name = ""
    local_files = fetch_file_paths_from_directory(source_folder_name)
    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )

    assert len(matches) == 4
    expected_results = [
        {
            "source_path": "reports/monthly/jan_data.csv",
            "destination_filename": "jan_data.csv",
        },
        {
            "source_path": "reports/monthly/feb_data.csv",
            "destination_filename": "feb_data.csv",
        },
        {
            "source_path": "reports/monthly/mar_data.csv",
            "destination_filename": "mar_data.csv",
        },
        {
            "source_path": "reports/aggregated/data_monthly.csv",
            "destination_filename": "data_monthly.csv",
        },
    ]
    assert matches == expected_results


def test_regex_match_enumerated_destination_files(test_file_system):
    source_file_match_type = "regex_match"
    source_file_name = "_data\.csv"
    source_folder_name = ""
    destination_file_name = "data.csv"
    destination_folder_name = ""
    local_files = fetch_file_paths_from_directory(source_folder_name)
    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )

    expected_results = [
        {
            "source_path": "reports/customer_data.csv",
            "destination_filename": "data_1.csv",
        },
        {
            "source_path": "reports/external/geo_data.csv",
            "destination_filename": "data_2.csv",
        },
        {
            "source_path": "reports/monthly/jan_data.csv",
            "destination_filename": "data_3.csv",
        },
        {
            "source_path": "reports/monthly/feb_data.csv",
            "destination_filename": "data_4.csv",
        },
        {
            "source_path": "reports/monthly/mar_data.csv",
            "destination_filename": "data_5.csv",
        },
        {"source_path": "company_data.csv", "destination_filename": "data_6.csv"},
    ]

    assert len(matches) == 6
    for expected_result in expected_results:
        # Which file that is named 1 2 3 etc. is not guaranteed
        assert expected_result["source_path"] in [
            match["source_path"] for match in matches
        ]
        assert expected_result["destination_filename"] in [
            match["destination_filename"] for match in matches
        ]
        assert os.path.exists(expected_result["source_path"])


def test_regex_match_non_enumerated_destination_files(test_file_system):
    source_file_match_type = "regex_match"
    source_file_name = "jan_.*"
    source_folder_name = ""
    destination_file_name = "data.csv"
    destination_folder_name = ""
    local_files = fetch_file_paths_from_directory(source_folder_name)

    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )
    assert len(matches) == 1
    assert matches[0]["source_path"] == "reports/monthly/jan_data.csv"
    assert matches[0]["destination_filename"] == "data.csv"
    assert os.path.exists(matches[0]["source_path"])


def test_regex_match_ignore_source_folder(test_file_system):
    source_file_match_type = "regex_match"
    source_file_name = "test"
    source_folder_name = "test"
    destination_file_name = ""
    destination_folder_name = ""
    local_files = fetch_file_paths_from_directory(source_folder_name)

    matches = file_match(
        search_term=source_file_name,
        source_directory=source_folder_name,
        destination_filename=destination_file_name,
        destination_directory=destination_folder_name,
        files=local_files,
        match_type=source_file_match_type,
    )

    assert len(matches) == 1
    assert matches[0]["source_path"] == "test/test.csv"
    assert matches[0]["destination_filename"] == "test.csv"
