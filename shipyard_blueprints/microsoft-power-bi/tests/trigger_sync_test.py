import os
import subprocess

import pytest
from dotenv import load_dotenv

from shipyard_microsoft_power_bi.microsoft_power_bi import MicrosoftPowerBiClient


@pytest.fixture(scope='class')
def load_env_dataflow():
    if os.path.exists("valid_dataflow.env"):
        load_dotenv("valid_dataflow.env")
    elif os.path.exists("tests/valid_dataflow.env"):
        load_dotenv("tests/valid_dataflow.env")
    else:
        pytest.skip("Could not find valid_dataflow.env")

    """Fixture to load environment variables for dataflow tests."""


@pytest.fixture(scope='class')
def load_env_dataset():
    """Fixture to load environment variables for dataset tests."""
    if os.path.exists("valid_dataset.env"):
        load_dotenv("valid_dataset.env")
    elif os.path.exists("tests/valid_dataset.env"):
        load_dotenv("tests/valid_dataset.env")
    else:
        pytest.skip("Could not find valid_dataset.env")


@pytest.fixture
def run_command():
    def _run_command():
        if os.path.exists("shipyard_microsoft_power_bi/cli/trigger_sync.py"):
            cmd = ["python3", "shipyard_microsoft_power_bi/cli/trigger_sync.py"]
        elif os.path.exists("../shipyard_microsoft_power_bi/cli/trigger_sync.py"):
            cmd = ["python3", "../shipyard_microsoft_power_bi/cli/trigger_sync.py"]
        else:
            pytest.skip("Could not find trigger_sync.py in the expected location")

        arg_list = [
            "--client-id", os.getenv("MICROSOFT_POWER_BI_CLIENT_ID", ""),  # Optional
            "--client-secret", os.getenv("MICROSOFT_POWER_BI_CLIENT_SECRET", ""),  # Optional
            "--tenant-id", os.getenv("MICROSOFT_POWER_BI_TENANT_ID", ""),  # Optional
            "--group-id", os.getenv("MICROSOFT_POWER_BI_GROUP_ID", ""),  # Required
            "--object-type", os.getenv("MICROSOFT_POWER_BI_OBJECT_TYPE", ""),  # Required
            "--object-id", os.getenv("MICROSOFT_POWER_BI_OBJECT_ID", ""),  # Required
            "--wait-for-completion", os.getenv("MICROSOFT_POWER_BI_WAIT_FOR_COMPLETION", "false"),
            "--poke-interval", "1"
        ]

        return subprocess.run(cmd + arg_list, capture_output=True, text=True)

    return _run_command


class TestDataflowSync:
    @pytest.fixture(autouse=True)
    def setup_env(self, load_env_dataflow):
        """Automatically load environment variables for dataflow tests."""
        pass

    def test_dataflow_sync_happy_path(self, run_command, monkeypatch):
        result = run_command()

        assert result.returncode == 0, f"Process failed: {result.stderr}"

    def test_dataflow_sync_bad_credentials(self, run_command, monkeypatch):
        monkeypatch.setenv("MICROSOFT_POWER_BI_CLIENT_ID", "")

        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with missing credentials."
        assert result.returncode == MicrosoftPowerBiClient.EXIT_CODE_INVALID_CREDENTIALS, "Unexpected exit code."

        monkeypatch.setenv("MICROSOFT_POWER_BI_CLIENT_ID", "bad_id")
        assert result.returncode != 0, "Process unexpectedly succeeded with bad credentials."
        assert result.returncode == MicrosoftPowerBiClient.EXIT_CODE_INVALID_CREDENTIALS, "Unexpected exit code."

    def test_dataflow_sync_bad_id(self, run_command, monkeypatch):
        monkeypatch.setenv("MICROSOFT_POWER_BI_OBJECT_ID", "bad_id")
        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with bad object_id."

        monkeypatch.setenv("MICROSOFT_POWER_BI_OBJECT_ID", "")
        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with missing object_id."


class TestDatasetSync:
    @pytest.fixture(autouse=True)
    def setup_env(self, load_env_dataset):
        """Automatically load environment variables for dataset tests."""
        pass

    def test_dataset_sync_happy_path(self, run_command):
        result = run_command()
        assert result.returncode == 0, f"Process failed: {result.stderr}"

    def test_dataset_sync_bad_credentials(self, run_command, monkeypatch):
        monkeypatch.setenv("MICROSOFT_POWER_BI_CLIENT_ID", "")

        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with missing credentials."
        assert result.returncode == MicrosoftPowerBiClient.EXIT_CODE_INVALID_CREDENTIALS, "Unexpected exit code."

        monkeypatch.setenv("MICROSOFT_POWER_BI_CLIENT_ID", "bad_id")

        assert result.returncode != 0, "Process unexpectedly succeeded with bad credentials."
        assert result.returncode == MicrosoftPowerBiClient.EXIT_CODE_INVALID_CREDENTIALS, "Unexpected exit code."

    def test_dataset_sync_bad_id(self, run_command, monkeypatch):
        monkeypatch.setenv("MICROSOFT_POWER_BI_OBJECT_ID", "bad_id")
        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with bad object_id."

        monkeypatch.setenv("MICROSOFT_POWER_BI_OBJECT_ID", "")
        result = run_command()

        assert result.returncode != 0, "Process unexpectedly succeeded with missing object_id."
