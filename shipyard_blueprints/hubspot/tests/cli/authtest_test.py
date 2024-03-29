import os
import pytest

from unittest.mock import patch

from dotenv import load_dotenv
from shipyard_hubspot.cli import authtest

####################################
# IMPORTANT NOTE:
# The .env file is used to make ACTUAL calls to the server API.
# This is to TEST real credentials. Ensure that sensitive information
# is handled appropriately!
####################################

if env_exists := os.path.exists(".env"):
    load_dotenv()


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_with_valid_credentials():
    with pytest.raises(SystemExit) as execinfo:
        authtest.main()
    assert execinfo.value.code == 0


@patch("shipyard_hubspot.cli.authtest.HubspotClient.connect", return_value=1)
def test_mocked_failed_connection(mock_connect):
    with pytest.raises(SystemExit) as execinfo:
        authtest.main()
    assert execinfo.value.code == 1


@patch("shipyard_hubspot.cli.authtest.HubspotClient.connect", return_value=0)
def test_mocked_successful_connection(mock_connect):
    with pytest.raises(SystemExit) as execinfo:
        authtest.main()
    assert execinfo.value.code == 0


def test_with_invalid_credentials():
    os.environ["HUBSPOT_ACCESS_TOKEN"] = "invalid_token"

    with pytest.raises(SystemExit) as execinfo:
        authtest.main()
    assert execinfo.value.code == 1


def test_with_no_credentials():
    os.environ["HUBSPOT_ACCESS_TOKEN"] = ""
    with pytest.raises(SystemExit) as execinfo:
        authtest.main()
    assert execinfo.value.code == 1
