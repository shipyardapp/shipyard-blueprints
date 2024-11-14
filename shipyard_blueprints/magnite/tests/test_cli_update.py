import pytest
import os
import subprocess

from dotenv import load_dotenv, find_dotenv
from shipyard_magnite import MagniteClient
from pytest import MonkeyPatch
from copy import deepcopy
from requests.exceptions import HTTPError

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def command():
    return [
        "python3",
        "./shipyard_magnite/cli/update.py",
        "--username",
        os.getenv("MAGNITE_USERNAME"),
        "--password",
        os.getenv("MAGNITE_PASSWORD"),
        "--endpoint",
        "campaigns",
        "--id",
        os.getenv("CAMPAIGN_ID"),
    ]


def test_update_cli(command):
    cmd = deepcopy(command)

    cmd.extend(["--budget-value", "500"])

    result = subprocess.run(cmd, capture_output=True)
    assert result.returncode == 0
