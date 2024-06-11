import pytest
import subprocess
import os
from copy import deepcopy
from dotenv import load_dotenv, find_dotenv

env_exists = 0
if env_exists := find_dotenv():
    load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def refresh_workbook():
    return [
        "python3",
        "./shipyard_tableau/cli/refresh_workbook.py",
        "--server-url",
        os.getenv("TABLEAU_SERVER_URL"),
        "--site-id",
        os.getenv("TABLEAU_SITE_ID"),
    ]


@pytest.fixture(scope="module")
def refresh_datasource():
    return [
        "python3",
        "./shipyard_tableau/cli/refresh_datasource.py",
        "--server-url",
        os.getenv("TABLEAU_SERVER_URL"),
        "--site-id",
        os.getenv("TABLEAU_SITE_ID"),
    ]


@pytest.fixture(scope="module")
def download_cmd():
    return [
        "python3",
        "./shipyard_tableau/cli/download_view.py",
        "--server-url",
        os.getenv("TABLEAU_SERVER_URL"),
        "--site-id",
        os.getenv("TABLEAU_SITE_ID"),
    ]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_refresh_workbook_PAT(refresh_workbook):
    cmd = deepcopy(refresh_workbook)
    cmd.extend(
        [
            "--sign-in-method",
            "access_token",
            "--project-name",
            "Orchestration Comparison",
            "--workbook-name",
            "Orchestration Comparison WKBK",
            "--check-status",
            "TRUE",
            "--access-token-name",
            os.getenv("ACCESS_TOKEN_NAME"),
            "--access-token-value",
            os.getenv("ACCESS_TOKEN_SECRET"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_refresh_datasource_PAT(refresh_datasource):
    cmd = deepcopy(refresh_datasource)
    cmd.extend(
        [
            "--sign-in-method",
            "access_token",
            "--project-name",
            "data_and_stuff",
            "--datasource-name",
            "AHOD",
            "--check-status",
            "TRUE",
            "--access-token-name",
            os.getenv("ACCESS_TOKEN_NAME"),
            "--access-token-value",
            os.getenv("ACCESS_TOKEN_SECRET"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_download_view_pat(download_cmd):
    cmd = deepcopy(download_cmd)
    cmd.extend(
        [
            "--sign-in-method",
            "access_token",
            "--project-name",
            "data_and_stuff",
            "--file-type",
            "png",
            "--destination-file-name",
            "ahod.png",
            "--workbook-name",
            "AHOD Submissions",
            "--view-name",
            "Sheet 1",
            "--access-token-name",
            os.getenv("ACCESS_TOKEN_NAME"),
            "--access-token-value",
            os.getenv("ACCESS_TOKEN_SECRET"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_downlaod_view_to_folder_pat(download_cmd):
    cmd = deepcopy(download_cmd)
    cmd.extend(
        [
            "--sign-in-method",
            "access_token",
            "--project-name",
            "data_and_stuff",
            "--file-type",
            "png",
            "--destination-folder-name",
            "test_folder",
            "--destination-file-name",
            "ahod_nested.png",
            "--workbook-name",
            "AHOD Submissions",
            "--view-name",
            "Sheet 1",
            "--access-token-name",
            os.getenv("ACCESS_TOKEN_NAME"),
            "--access-token-value",
            os.getenv("ACCESS_TOKEN_SECRET"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_file_exists():
    assert os.path.exists("ahod.png")
    assert os.path.exists("test_folder/ahod_nested.png")
    os.remove("ahod.png")
    subprocess.run(["rm", "-rf", "test_folder"])


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_refresh_workbook_jwt(refresh_workbook):
    cmd = deepcopy(refresh_workbook)
    cmd.extend(
        [
            "--sign-in-method",
            "jwt",
            "--project-name",
            "Orchestration Comparison",
            "--workbook-name",
            "Orchestration Comparison WKBK",
            "--check-status",
            "TRUE",
            "--client-id",
            os.getenv("CLIENT_ID"),
            "--client-secret",
            os.getenv("CLIENT_SECRET"),
            "--secret-value",
            os.getenv("SECRET_VALUE"),
            "--username",
            os.getenv("TABLEAU_USERNAME"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_refresh_datasource_jwt(refresh_datasource):
    cmd = deepcopy(refresh_datasource)
    cmd.extend(
        [
            "--sign-in-method",
            "jwt",
            "--project-name",
            "data_and_stuff",
            "--datasource-name",
            "AHOD",
            "--check-status",
            "TRUE",
            "--client-id",
            os.getenv("CLIENT_ID"),
            "--client-secret",
            os.getenv("CLIENT_SECRET"),
            "--secret-value",
            os.getenv("SECRET_VALUE"),
            "--username",
            os.getenv("TABLEAU_USERNAME"),
        ]
    )
    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_download_view_jwt(download_cmd):
    cmd = deepcopy(download_cmd)
    cmd.extend(
        [
            "--sign-in-method",
            "jwt",
            "--project-name",
            "data_and_stuff",
            "--file-type",
            "png",
            "--destination-file-name",
            "ahod.png",
            "--workbook-name",
            "AHOD Submissions",
            "--view-name",
            "Sheet 1",
            "--client-id",
            os.getenv("CLIENT_ID"),
            "--client-secret",
            os.getenv("CLIENT_SECRET"),
            "--secret-value",
            os.getenv("SECRET_VALUE"),
            "--username",
            os.getenv("TABLEAU_USERNAME"),
        ]
    )

    result = subprocess.run(cmd, capture_output=True)
    print(result.stderr)
    assert result.returncode == 0
