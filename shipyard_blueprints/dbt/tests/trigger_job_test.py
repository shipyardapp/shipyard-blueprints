import os
import subprocess
import shutil

import pytest
from dotenv import load_dotenv
from shipyard_templates import Etl

from shipyard_dbt.exceptions import EXIT_CODE_INVALID_RESOURCE

default_artifact_folder = f'{os.environ.get("USER")}-artifacts'


@pytest.fixture(scope="module", autouse=True)
def command():
    load_dotenv()
    if any(
        key not in os.environ
        for key in [
            "DBT_API_KEY",
            "DBT_ACCOUNT_ID",
            "DBT_JOB_ID",
        ]
    ):
        pytest.skip("Missing one or more required environment variables")

    def refresh_command():
        """
        It is necessary to define it this way so that the environment variables can be pulled at the time of execution
        rather than at the time of definition.
        """

        return [
            "python3",
            "shipyard_dbt/cli/trigger_job.py",
            "--api-key",
            os.getenv("DBT_API_KEY", ""),
            "--account-id",
            os.getenv("DBT_ACCOUNT_ID", ""),
            "--job-id",
            os.getenv("DBT_JOB_ID", ""),
            "--wait-for-completion",
            os.getenv("DBT_WAIT", "TRUE"),
        ]

    yield refresh_command

    if os.path.exists(default_artifact_folder):
        shutil.rmtree(default_artifact_folder)


def test_valid_run(command):
    test = subprocess.run(command(), capture_output=True)
    assert test.returncode == 0


def test_invalid_credentials(command, monkeypatch):
    monkeypatch.setenv("DBT_API_KEY", "bad_key")
    test = subprocess.run(command(), capture_output=True)
    assert test.returncode == Etl.EXIT_CODE_INVALID_CREDENTIALS


@pytest.mark.parametrize(
    "mock_job_id,expected_exit_code",
    [
        ("12345", EXIT_CODE_INVALID_RESOURCE),
        ("bad_job", Etl.EXIT_CODE_BAD_REQUEST),
        ("", Etl.EXIT_CODE_BAD_REQUEST),
    ],
)
def test_invalid_job(command, monkeypatch, mock_job_id, expected_exit_code):
    monkeypatch.setenv("DBT_JOB_ID", mock_job_id)
    test = subprocess.run(command(), capture_output=True)
    assert test.returncode == expected_exit_code


def test_without_wait(command, monkeypatch):
    monkeypatch.setenv("DBT_WAIT", "FALSE")
    test = subprocess.run(command(), capture_output=True)
    assert test.returncode == 0
