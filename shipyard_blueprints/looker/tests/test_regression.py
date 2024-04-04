import pytest
import subprocess
import os
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "client_id": os.getenv("LOOKER_CLIENT_ID"),
        "client_secret": os.getenv("LOOKER_CLIENT_SECRET"),
        "base_url": os.getenv("LOOKER_URL"),
    }


@pytest.fixture(scope="module")
def dash_cmd(creds):
    return [
        "python3",
        "./shipyard_looker/cli/download_dashboard.py",
        "--base-url",
        creds["base_url"],
        "--client-id",
        creds["client_id"],
        "--client-secret",
        creds["client_secret"],
        "--dashboard-id",
        os.getenv("DASHBOARD_ID"),
        "--output-width",
        "800",
        "--output-height",
        "600",
    ]


@pytest.fixture(scope="module")
def look_cmd(creds):
    return [
        "python3",
        "./shipyard_looker/cli/download_look_as_file.py",
        "--base-url",
        creds["base_url"],
        "--client-id",
        creds["client_id"],
        "--client-secret",
        creds["client_secret"],
        "--look-id",
        os.getenv("LOOK_ID"),
    ]


@pytest.fixture(scope="module")
def sql_cmd(creds):
    return [
        "python3",
        "./shipyard_looker/cli/create_sql_query.py",
        "--base-url",
        creds["base_url"],
        "--client-id",
        creds["client_id"],
        "--client-secret",
        creds["client_secret"],
    ]


@pytest.fixture(scope="module")
def run_sql_cmd(creds):
    return [
        "python3",
        "./shipyard_looker/cli/run_sql_query.py",
        "--base-url",
        creds["base_url"],
        "--client-id",
        creds["client_id"],
        "--client-secret",
        creds["client_secret"],
    ]


def test_download_dashboard(dash_cmd):
    cmd = deepcopy(dash_cmd)
    cmd.extend(["--destination-file-name", "test_dashboard.png", "--file-type", "png"])
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_dashboard_to_folder(dash_cmd):
    cmd = deepcopy(dash_cmd)
    cmd.extend(
        [
            "--destination-folder-name",
            "dashboard_folder",
            "--destination-file-name",
            "test_dashboard.png",
            "--file-type",
            "png",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_look(look_cmd):
    cmd = deepcopy(look_cmd)
    cmd.extend(["--destination-file-name", "test_look.png", "--file-type", "png"])
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_look_to_folder(look_cmd):
    cmd = deepcopy(look_cmd)
    cmd.extend(
        [
            "--destination-folder-name",
            "look_folder",
            "--destination-file-name",
            "test_look.png",
            "--file-type",
            "png",
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_sql(run_sql_cmd):
    cmd = deepcopy(run_sql_cmd)
    cmd.extend(
        [
            "--destination-file-name",
            "sql.csv",
            "--file-type",
            "csv",
            "--slug",
            os.getenv("SQL_SLUG"),
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_download_sql_to_folder(run_sql_cmd):
    cmd = deepcopy(run_sql_cmd)
    cmd.extend(
        [
            "--destination-folder-name",
            "sql_folder",
            "--destination-file-name",
            "sql.csv",
            "--file-type",
            "csv",
            "--slug",
            os.getenv("SQL_SLUG"),
        ]
    )
    process = subprocess.run(cmd, capture_output=True)
    assert process.returncode == 0


def test_cleanup():
    os.remove("test_dashboard.png")
    os.remove("dashboard_folder/test_dashboard.png")
    os.remove("test_look.png")
    os.remove("look_folder/test_look.png")
    os.removedirs("dashboard_folder")
    os.removedirs("look_folder")
    os.remove("sql.csv")
    os.remove("sql_folder/sql.csv")
    os.removedirs("sql_folder")

    assert not os.path.exists("test_dashboard.png")
    assert not os.path.exists("dashboard_folder")
    assert not os.path.exists("test_look.png")
    assert not os.path.exists("look_folder")
    assert not os.path.exists("sql.csv")
    assert not os.path.exists("sql_folder")
