import os

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_dbt.cli.authtest import main

CREDENTIALS = ["DBT_API_KEY", "DBT_ACCOUNT_ID"]

INVALID_INPUT = ["INVALID", 123, ""]


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in CREDENTIALS
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials(monkeypatch):
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_input", INVALID_INPUT)
def test_invalid_api_key(invalid_input, monkeypatch):
    monkeypatch.setenv("DBT_API_KEY", invalid_input)

    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_input", ["INVALID", 123])
def test_invalid_account_id(invalid_input, monkeypatch):
    # The account id is technically not required for a successful authtest but if it is invalid, the test should fail
    monkeypatch.setenv("DBT_ACCOUNT_ID", invalid_input)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


def test_invalid_with_empty_string_account_id(monkeypatch):
    # The account id is technically not required for a successful authtest. If it is empty, the test should still pass
    monkeypatch.setenv("DBT_ACCOUNT_ID", "")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("missing_env", CREDENTIALS)
def test_missing_env(missing_env, monkeypatch):
    # Although the account id is not required for the authtest it is required for initializing the client. If None
    # the authtest should fail
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
