import pytest
import os

from dotenv import load_dotenv, find_dotenv
from shipyard_magnite import MagniteClient
from pytest import MonkeyPatch
from copy import deepcopy

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "username": os.getenv("MAGNITE_USERNAME"),
        "password": os.getenv("MAGNITE_PASSWORD"),
    }


## test connect
def test_connect_successful(creds):
    client = MagniteClient(**creds)
    res = client.connect()
    assert client.token is not None
    assert res == 0


def test_connect_bad_username(creds):
    creds_copy = deepcopy(creds)
    creds_copy["username"] = "bad_user"
    client = MagniteClient(**creds_copy)
    assert client.connect() == 1


def test_connect_bad_password(creds):
    creds_copy = deepcopy(creds)
    creds_copy["password"] = "bad_password"
    client = MagniteClient(**creds_copy)
    assert client.connect() == 1


def test_connect_bad_username_and_bad_password(creds):
    creds_copy = deepcopy(creds)
    creds_copy["username"] = "bad_user"
    creds_copy["password"] = "bad_password"
    client = MagniteClient(**creds_copy)
    assert client.connect() == 1
