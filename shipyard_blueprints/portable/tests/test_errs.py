import subprocess
import os
import pytest
from pytest import MonkeyPatch
from dotenv import load_dotenv, find_dotenv
from copy import deepcopy
from shipyard_templates import Etl
from shipyard_templates.etl import UnauthorizedError, BadRequestError
from shipyard_portable import PortableClient

if env_exists := os.path.exists(".env"):
    load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def trigger():
    return [
        "python3",
        "./shipyard_portable/cli/trigger.py",
        "--access-token",
        os.getenv("PORTABLE_API_TOKEN"),
    ]


def test_trigger_bad_api_key():
    client = PortableClient(access_token="bad_api_token")
    flow_id = os.getenv("PORTABLE_FLOW_1")
    with pytest.raises(UnauthorizedError):
        client.trigger_sync(flow_id=flow_id)


def test_trigger_bad_flow_id():
    client = PortableClient(access_token=os.getenv("PORTABLE_API_TOKEN"))
    flow_id = "bad_flow_id"
    with pytest.raises(BadRequestError):
        client.trigger_sync(flow_id=flow_id)


def test_get_flow_status_bad_api_key():
    client = PortableClient(access_token="bad_api_token")
    flow_id = os.getenv("PORTABLE_FLOW_1")
    with pytest.raises(UnauthorizedError):
        client.get_sync_status(flow_id=flow_id)


def test_get_flow_status_bad_flow_id():
    client = PortableClient(access_token=os.getenv("PORTABLE_API_TOKEN"))
    flow_id = "bad_flow_id"
    with pytest.raises(BadRequestError):
        client.get_sync_status(flow_id=flow_id)
