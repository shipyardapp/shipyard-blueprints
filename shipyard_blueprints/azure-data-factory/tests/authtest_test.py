import os

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_azure_data_factory.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
        key not in os.environ
        for key in [
            "AZURE_DATAFACTORY_CLIENT_ID",
            "AZURE_DATAFACTORY_CLIENT_SECRET",
            "AZURE_DATAFACTORY_TENANT_ID",
            "AZURE_DATAFACTORY_SUBSCRIPTION_ID",
        ]
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_credentials(monkeypatch):
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_value", ["bad", ""])
def test_invalid_secret(invalid_value, monkeypatch):
    monkeypatch.setenv("AZURE_DATAFACTORY_CLIENT_SECRET", invalid_value)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_value", ["bad", ""])
def test_invalid_client_id(invalid_value, monkeypatch):
    monkeypatch.setenv("AZURE_DATAFACTORY_CLIENT_ID", invalid_value)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_value", ["bad", ""])
def test_invalid_tenant_id(invalid_value, monkeypatch):
    monkeypatch.setenv("AZURE_DATAFACTORY_TENANT_ID", invalid_value)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_value", ["bad", ""])
def test_invalid_subscription_id(invalid_value, monkeypatch):
    monkeypatch.setenv("AZURE_DATAFACTORY_SUBSCRIPTION_ID", invalid_value)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize(
    "missing_env",
    [
        "AZURE_DATAFACTORY_CLIENT_ID",
        "AZURE_DATAFACTORY_CLIENT_SECRET",
        "AZURE_DATAFACTORY_TENANT_ID",
        "AZURE_DATAFACTORY_SUBSCRIPTION_ID",
    ],
)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
