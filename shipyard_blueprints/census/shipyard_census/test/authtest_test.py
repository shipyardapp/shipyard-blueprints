import os

import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_census.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in ["CENSUS_API_KEY"]
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_key", ["bad_key", ""])
def test_invalid_api_key(invalid_key, monkeypatch):
    monkeypatch.setenv("CENSUS_API_KEY", invalid_key)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1
