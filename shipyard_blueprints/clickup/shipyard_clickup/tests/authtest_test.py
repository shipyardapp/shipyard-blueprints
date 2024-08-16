import os

import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_clickup.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(key not in os.environ for key in ["CLICKUP_ACCESS_TOKEN"]):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_access_token", ["bad_access_token", "", None])
def test_invalid_password(invalid_access_token, monkeypatch):
    monkeypatch.setenv("CLICKUP_ACCESS_TOKEN", invalid_access_token)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1
