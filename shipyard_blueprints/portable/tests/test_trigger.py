import subprocess
import os
import pytest
from pytest import MonkeyPatch
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy
from shipyard_templates import Etl

if env_exists := os.path.exists(".env"):
    load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def trigger():
    return [
        "python3",
        "./shipyard_portable/cli/trigger.py",
        "--access-token",
        os.getenv("PORTABLE_API_TOKEN"),
    ]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_trigger_no_wait(trigger):
    cmd = deepcopy(trigger)
    flow_id = os.getenv("PORTABLE_FLOW_1")
    cmd.extend(["--flow-id", flow_id, "--wait-for-completion", "FALSE"])
    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == Etl.EXIT_CODE_FINAL_STATUS_PENDING


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_trigger_wait(trigger):
    cmd = deepcopy(trigger)
    flow_id = os.getenv("PORTABLE_FLOW_2")
    cmd.extend(["--flow-id", flow_id, "--wait-for-completion", "TRUE"])
    result = subprocess.run(cmd, capture_output=True)

    assert result.returncode == Etl.EXIT_CODE_FINAL_STATUS_COMPLETED
