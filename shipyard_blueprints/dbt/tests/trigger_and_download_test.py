import os
import shutil
import subprocess

import pytest
from dotenv import load_dotenv

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

        return ["python3",
                "shipyard_dbt/cli/trigger_and_download.py",
                "--api-key", os.getenv("DBT_API_KEY", ""),
                "--account-id", os.getenv("DBT_ACCOUNT_ID", ""),
                "--job-id", os.getenv("DBT_JOB_ID", ""),
                "--download-artifacts", "TRUE",
                "--download-logs", "TRUE"]

    yield refresh_command

    if os.path.exists(default_artifact_folder):
        shutil.rmtree(default_artifact_folder)


def test_valid_run(command):
    test = subprocess.run(command(), capture_output=True)
    assert test.returncode == 0
    assert os.path.exists(f"{default_artifact_folder}/logs/dbt.log")
    assert os.path.exists(f"{default_artifact_folder}/logs/dbt_console_output.txt")
    assert os.path.exists(f"{default_artifact_folder}/artifacts")
