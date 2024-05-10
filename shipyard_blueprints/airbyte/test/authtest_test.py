import os
import subprocess

import pytest
from dotenv import load_dotenv


def run_cli_command():
    return subprocess.run(
        ['python3', "shipyard_airbyte/cli/authtest.py"],
        capture_output=True,
        text=True,
    )


# Function to load environment variables from .env file
@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    if not load_dotenv():
        pytest.skip("No .env file found")

    credentials = ("AIRBYTE_API_TOKEN",)

    if not all(os.getenv(credential) for credential in credentials):
        pytest.skip("Missing required credentials in .env file")

    for key in credentials:
        monkeypatch.setenv(key, os.getenv(key))


def test_valid_credentials():
    result = run_cli_command()
    assert result.returncode == 0, "CLI should pass with valid credentials"


def test_invalid_credential(monkeypatch):
    monkeypatch.setenv("AIRBYTE_API_TOKEN", "invalid_token")
    result = run_cli_command()
    assert result.returncode != 0, f"CLI should fail with invalid credentials, got {result.returncode}"


def test_missing_credentials(monkeypatch):
    monkeypatch.delenv("AIRBYTE_API_TOKEN")
    result = run_cli_command()
    assert result.returncode != 0, f"CLI should fail with missing credentials, got {result.returncode}"
