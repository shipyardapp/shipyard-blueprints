import os
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_templates import ExitCodeException
from shipyard_domo import DomoClient
from typing import Dict

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def domo_creds():
    return {
        "DOMO_CLIENT_ID": os.getenv("DOMO_CLIENT_ID"),
        "DOMO_SECRET_KEY": os.getenv("DOMO_SECRET_KEY"),
        "DOMO_ACCESS_TOKEN": os.getenv("DOMO_ACCESS_TOKEN"),
        "DOMO_INSTANCE": os.getenv("DOMO_INSTANCE"),
    }


@pytest.fixture(scope="module")
def good_client(domo_creds: Dict[str, str]) -> DomoClient:
    return DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )


def conn_helper(client: DomoClient):
    try:
        client.connect()
    except (Exception, ExitCodeException):
        return 1
    else:
        return 0


def client_secret_helper(client: DomoClient):
    try:
        client.connect_with_client_id_and_secret_key()
    except (Exception, ExitCodeException):
        return 1
    else:
        return 0


def token_instance_helper(client: DomoClient):
    try:
        client.connect_with_access_token()
    except (Exception, ExitCodeException):
        return 1
    else:
        return 0


def test_auth_good(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = conn_helper(client)
    assert res == 0


def test_auth_bad_client(domo_creds):
    client = DomoClient(
        client_id="bad_client",
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = conn_helper(client)
    assert res == 1


def test_auth_bad_secret(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key="bad_secret",
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = conn_helper(client)
    assert res == 1


def test_auth_bad_access_token(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token="bad_token",
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = conn_helper(client)
    print(f"res is {res}")
    assert res == 1


def test_auth_bad_instance(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance="bad_instance",
    )
    res = conn_helper(client)
    assert res == 1


def test_auth_client_and_secret(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = client_secret_helper(client)
    assert res == 0


def test_auth_token_instance_helper(domo_creds):
    client = DomoClient(
        client_id=domo_creds["DOMO_CLIENT_ID"],
        secret_key=domo_creds["DOMO_SECRET_KEY"],
        access_token=domo_creds["DOMO_ACCESS_TOKEN"],
        domo_instance=domo_creds["DOMO_INSTANCE"],
    )
    res = token_instance_helper(client)
    assert res == 0
