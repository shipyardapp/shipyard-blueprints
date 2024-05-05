import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy


load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def trigger_cmd():
    return [
        "python3",
        "./shipyard_api/cli/trigger_fleet.py",
        "--api-key",
        os.getenv("SHIPYARD_API_TOKEN"),
        "--organization-id",
        os.getenv("ORG_ID"),
        "--fleet-id",
        os.getenv("PROD_FLEET_ID"),
        "--project-id",
        os.getenv("PROD_PROJECT"),
    ]


@pytest.fixture(scope="module")
def artifact_path():
    artifact_folder_default = f'{os.environ.get("USER")}-artifacts'
    return os.path.join(artifact_folder_default, "shipyard-api-blueprints")


def test_trigger_fleet(trigger_cmd):
    process = subprocess.run(trigger_cmd, capture_output=True)
    assert process.returncode == 0
    # check that the variables and responses have been properly set


def test_artifact_check(artifact_path):
    assert os.path.exists(os.path.join(artifact_path))
    # assert os.path.exists(os.path.join(artifact_path, 'variables'))
