import os

import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_databricks.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in [
                "DATABRICKS_ACCESS_TOKEN",
                "DATABRICKS_INSTANCE_URL",
            ]
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_access_token", ["invalid_access_token", ""])
def test_invalid_password(invalid_access_token, monkeypatch):
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", invalid_access_token)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_username", ["bad_username", ""])
def test_invalid_username(invalid_username, monkeypatch):
    monkeypatch.setenv("SFTP_USERNAME", invalid_username)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_url", ["bad_url", ""])
def test_invalid_host(invalid_url, monkeypatch):
    monkeypatch.setenv("DATABRICKS_INSTANCE_URL", invalid_url)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1

@pytest.mark.parametrize(
    "missing_env", ["DATABRICKS_ACCESS_TOKEN", "DATABRICKS_INSTANCE_URL"]
)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
