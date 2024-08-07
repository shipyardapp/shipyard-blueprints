import os
import shutil
import subprocess

import pytest
import requests
from dotenv import load_dotenv, find_dotenv


@pytest.fixture(scope="session", autouse=True)
def load_environment():
    """Fixture to load environment variables and skip tests if .env file is not found."""
    if dotenv_path := find_dotenv():
        load_dotenv(dotenv_path)
    else:
        pytest.skip("No .env file found, skipping tests.")


@pytest.fixture(scope="session")
def csv_file():
    response = requests.get(
        "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
    )
    if response.ok:
        with open("electric-vehicle-population-data.csv", "wb") as f:
            f.write(response.content)

        os.makedirs("test_files", exist_ok=True)
        shutil.copy(
            "electric-vehicle-population-data.csv",
            "test_files/electric-vehicle-population-data_copy.csv",
        )

    else:
        pytest.mark.skip("Failed to download CSV file for testing")

    yield

    os.remove("electric-vehicle-population-data.csv")
    shutil.rmtree("test_files")


@pytest.fixture
def run_command():
    def _run_command():
        # One Auth method is required currently either access token or client id, client secret and tenant
        arg_list = [
            "--access-token",
            os.getenv("OAUTH_ACCESS_TOKEN", ""),  # Optional
            "--client-id",
            os.getenv("SHAREPOINT_CLIENT_ID", ""),  # Optional
            "--client-secret",
            os.getenv("SHAREPOINT_CLIENT_SECRET", ""),  # Optional
            "--tenant",
            os.getenv("SHAREPOINT_TENANT", ""),  # Optional
            "--site-name",
            os.getenv("SHAREPOINT_SITE_NAME", ""),  # Required
            "--file-name",
            os.getenv("SHAREPOINT_LOCAL_FILE_NAME", ""),  # Required
            "--directory",
            os.getenv("SHAREPOINT_LOCAL_FOLDER_NAME", ""),  # Optional
            "--sharepoint-file-name",
            os.getenv("SHAREPOINT_FILE_NAME", ""),  # Optional
            "--sharepoint-directory",
            os.getenv("SHAREPOINT_FOLDER_NAME", ""),  # Optional
            "--match-type",
            os.getenv("SHAREPOINT_FILE_MATCH_TYPE", ""),  # Optional
        ]
        if os.path.exists("shipyard_microsoft_sharepoint/cli/upload.py"):
            cmd = ["python3", "shipyard_microsoft_sharepoint/cli/upload.py"]
        elif os.path.exists("../shipyard_microsoft_sharepoint/cli/upload.py"):
            cmd = ["python3", "../shipyard_microsoft_sharepoint/cli/upload.py"]
        else:
            pytest.skip("Could not find upload.py in the expected location")
        return subprocess.run(cmd + arg_list, capture_output=True)

    return _run_command


def test_exact_match_from_cwd_to_default_target(run_command, monkeypatch, csv_file):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "exact_match")
    monkeypatch.setenv(
        "SHAREPOINT_LOCAL_FILE_NAME", "electric-vehicle-population-data.csv"
    )
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")

    result = run_command()
    assert result.returncode == 0


def test_exact_match_from_subfolder_to_default_target(
    run_command, monkeypatch, csv_file
):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "exact_match")
    monkeypatch.setenv(
        "SHAREPOINT_LOCAL_FILE_NAME", "electric-vehicle-population-data_copy.csv"
    )
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")
    monkeypatch.setenv("SHAREPOINT_LOCAL_FOLDER_NAME", "test_files")

    result = run_command()
    assert result.returncode == 0


def test_exact_match_from_cwd_to_target_dir(run_command, monkeypatch, csv_file):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "exact_match")
    monkeypatch.setenv(
        "SHAREPOINT_LOCAL_FILE_NAME", "electric-vehicle-population-data.csv"
    )
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")
    monkeypatch.setenv("SHAREPOINT_FOLDER_NAME", "pytest")

    result = run_command()
    assert result.returncode == 0


def test_exact_match_from_subfolder_to_target_dir(run_command, monkeypatch, csv_file):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "exact_match")
    monkeypatch.setenv(
        "SHAREPOINT_LOCAL_FILE_NAME", "electric-vehicle-population-data_copy.csv"
    )
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")
    monkeypatch.setenv("SHAREPOINT_LOCAL_FOLDER_NAME", "test_files")

    result = run_command()
    assert result.returncode == 0


def test_regex_match_from_cwd_to_default_target(run_command, monkeypatch, csv_file):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "regex_match")
    monkeypatch.setenv("SHAREPOINT_LOCAL_FILE_NAME", "electric-vehicle-population-data")
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")

    result = run_command()
    assert result.returncode == 0


def test_exact_match_file_does_not_exist(run_command, monkeypatch):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "exact_match")
    monkeypatch.setenv("SHAREPOINT_LOCAL_FILE_NAME", "non_existent_file.csv")
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")

    result = run_command()
    assert result.returncode == 207


def test_regex_match_file_does_not_exist(run_command, monkeypatch):
    monkeypatch.setenv("SHAREPOINT_FILE_MATCH_TYPE", "regex_match")
    monkeypatch.setenv("SHAREPOINT_LOCAL_FILE_NAME", "non_existent_file")
    monkeypatch.setenv("SHAREPOINT_SITE_NAME", "Shipyard")

    result = run_command()
    assert result.returncode == 207
