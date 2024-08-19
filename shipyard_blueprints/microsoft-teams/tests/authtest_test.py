import os

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_microsoft_teams.cli.authtest import main

CREDENTIALS = ["MICROSOFT_TEAMS_WEBHOOK_URL"]

INVALID_INPUT = [
    "INVALID",
    123,
    "",
    "https://shipyardapp.webhook.office.com/webhookb2/invalid_webhook_url",
]


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(key not in os.environ for key in CREDENTIALS):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials(monkeypatch):
    monkeypatch.setenv("OAUTH_ACCESS_TOKEN", "")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("credential", CREDENTIALS)
@pytest.mark.parametrize("invalid_input", INVALID_INPUT)
def test_invalid_credentials(credential, invalid_input, monkeypatch):
    monkeypatch.setenv("OAUTH_ACCESS_TOKEN", "")
    monkeypatch.setenv(credential, invalid_input)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("missing_env", CREDENTIALS)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.setenv("OAUTH_ACCESS_TOKEN", "")
    monkeypatch.delenv(missing_env, raising=False)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
