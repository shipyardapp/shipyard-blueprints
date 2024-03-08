import os
import pytest
import subprocess
from dotenv import load_dotenv, find_dotenv
from shipyard_hex import HexClient
import shipyard_bp_utils as shipyard
from shipyard_bp_utils.artifacts import Artifact

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "HEX_API_TOKEN": os.getenv("HEX_API_TOKEN"),
        "PROJECT_ID": os.getenv("PROJECT_ID"),
    }


def test_trigger_project(creds):
    run_cmd = [
        "python3",
        "./shipyard_hex/cli/hex_run_project.py",
        "--project-id",
        creds["PROJECT_ID"],
        "--api-token",
        creds["HEX_API_TOKEN"],
        "--wait-for-completion",
        "TRUE",
    ]

    process = subprocess.run(run_cmd)

    assert process.returncode == 0


def test_trigger_project_no_wait(creds):
    run_cmd = [
        "python3",
        "./shipyard_hex/cli/hex_run_project.py",
        "--project-id",
        creds["PROJECT_ID"],
        "--api-token",
        creds["HEX_API_TOKEN"],
        "--wait-for-completion",
        "FALSE",
    ]

    process = subprocess.run(run_cmd)

    assert process.returncode == 0
