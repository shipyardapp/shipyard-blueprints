import os

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_azureblob.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(key not in os.environ for key in ["AZURE_STORAGE_CONNECTION_STRING"]):
        pytest.skip("Missing one or more required environment variables.")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_connection", ["bad_string", ""])
def test_invalid_password(invalid_connection, monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", invalid_connection)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("missing_env", ["AZURE_STORAGE_CONNECTION_STRING"])
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
