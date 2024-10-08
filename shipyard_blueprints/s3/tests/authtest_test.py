import os

import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_s3.cli.authtest import main

CREDENTIALS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]

INVALID_INPUT = ["INVALID", 123, ""]


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(key not in os.environ for key in CREDENTIALS):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_input", INVALID_INPUT)
def test_invalid_access_key_id(invalid_input, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", invalid_input)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_input", INVALID_INPUT)
def test_invalid_secret_access_key(invalid_input, monkeypatch):
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", invalid_input)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("missing_env", CREDENTIALS)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
