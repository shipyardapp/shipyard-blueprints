import os

import pytest
from dotenv import load_dotenv, find_dotenv

from shipyard_sftp.cli.authtest import main


@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in [
                "SFTP_HOST",
                "SFTP_PORT",
                "SFTP_USERNAME",
                "SFTP_PASSWORD",
                "SFTP_RSA_KEY_FILE"
            ]
    ):
        pytest.skip("Missing one or more required environment variables")


def test_valid_un_pw_credentials(monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


def test_valid_pk_credentials():
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 0


@pytest.mark.parametrize("invalid_password", ["bad_password", ""])
def test_invalid_password(invalid_password, monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    monkeypatch.setenv("SFTP_PASSWORD", invalid_password)
    with pytest.raises(SystemExit) as exit_code:
        main()

    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_username", ["bad_username", ""])
def test_invalid_username(invalid_username, monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    monkeypatch.setenv("SFTP_USERNAME", invalid_username)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_host", ["bad_host", ""])
def test_invalid_host(invalid_host, monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    monkeypatch.setenv("SFTP_HOST", invalid_host)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize("invalid_port", ["bad_port", ""])
def test_invalid_port(invalid_port, monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    monkeypatch.setenv("SFTP_PORT", invalid_port)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


@pytest.mark.parametrize(
    "missing_env", ["SFTP_HOST", "SFTP_PORT", "SFTP_USERNAME", "SFTP_PASSWORD"]
)
def test_missing_env(missing_env, monkeypatch):
    monkeypatch.delenv("SFTP_RSA_KEY_FILE")
    monkeypatch.delenv(missing_env)
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1


def test_empty_string_key(monkeypatch):
    monkeypatch.setenv("SFTP_RSA_KEY_FILE", "")
    monkeypatch.delenv("SFTP_PASSWORD")
    with pytest.raises(SystemExit) as exit_code:
        main()
    assert exit_code.value.code == 1
