import os

import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_bigquery.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
        key not in os.environ
        for key in [
            "GOOGLE_APPLICATION_CREDENTIALS",
        ]
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


def test_missing_env(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


def test_invalid_credentials(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "bad_credentials")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
