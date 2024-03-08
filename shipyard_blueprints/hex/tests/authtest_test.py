import os
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_hex import HexClient


load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {"token": os.getenv("HEX_API_TOKEN"), "project_id": os.getenv("PROJECT_ID")}


def test_auth(creds):
    client = HexClient(api_token=creds["token"])
    assert client.connect(creds["project_id"]) == 0


def test_auth_bad(creds):
    client = HexClient(api_token=creds["token"])
    assert client.connect("bad-project-id") == 1


def test_auth_bad_token(creds):
    client = HexClient(api_token="bad-api-token")
    assert client.connect(creds["project_id"]) == 1
