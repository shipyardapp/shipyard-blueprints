import os
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_microsoft_power_bi.cli.authtest import main

CREDENTIALS = [
    "MICROSOFT_POWER_BI_CLIENT_ID",
    "MICROSOFT_POWER_BI_CLIENT_SECRET",
    "MICROSOFT_POWER_BI_TENANT_ID",
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


@pytest.mark.parametrize("credential", CREDENTIALS)
@pytest.mark.parametrize("invalid_input", INVALID_INPUT)
def test_invalid_credentials(credential, invalid_input, monkeypatch):
    monkeypatch.setenv(credential, invalid_input)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("missing_env", CREDENTIALS)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv(missing_env, raising=False)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
