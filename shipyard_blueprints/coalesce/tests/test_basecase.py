import pytest
import os
import subprocess
from dotenv import load_dotenv, find_dotenv
from shipyard_coalesce import CoalesceClient
from typing import Dict, List
from copy import deepcopy

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds() -> Dict[str, str]:
    return {
        "token": os.getenv("COALESCE_API_TOKEN"),
        "env": os.getenv("COALESCE_ENV_ID"),
        "param": os.getenv("COALESCE_PARAM"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "pwd": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "db": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }


@pytest.fixture(scope="module")
def cmd(creds) -> List[str]:
    coalesce = [
        "python3",
        "./shipyard_coalesce/cli/trigger_sync.py",
        "--access-token",
        creds["token"],
        "--environment-id",
        creds["env"],
        "--snowflake-username",
        creds["user"],
        "--snowflake-password",
        creds["pwd"],
        "--snowflake-warehouse",
        creds["warehouse"],
    ]
    return coalesce


def test_basecase(creds, cmd):
    coa = deepcopy(cmd)

    process = subprocess.run(coa)
    assert process.returncode == 0


def test_params(creds, cmd):
    coa = deepcopy(cmd)
    coa.extend(["--parameters", creds["param"], "--wait-for-completion", "TRUE"])

    process = subprocess.run(coa)
    assert process.returncode == 0


def test_param_change_default(creds, cmd):
    coa = deepcopy(cmd)
    coa.extend(
        ["--parameters", '{"test_param": "NEW VALUE"}', "--wait-for-completion", "TRUE"]
    )

    process = subprocess.run(coa)
    assert process.returncode == 0
