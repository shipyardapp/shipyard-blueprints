import pytest
import os

from dotenv import load_dotenv, find_dotenv
from shipyard_magnite import MagniteClient
from pytest import MonkeyPatch
from copy import deepcopy
from requests.exceptions import HTTPError

load_dotenv(find_dotenv())
BUDGET_VALUE = 100


@pytest.fixture(scope="module")
def client():
    user = os.getenv("MAGNITE_USERNAME")
    pwd = os.getenv("MAGNITE_PASSWORD")
    return MagniteClient(user, pwd)


def test_successful_update(client):
    client.connect()
    res = client.update(
        endpoint="campaigns",
        id=os.getenv("CAMPAIGN_ID"),
        budget_value=BUDGET_VALUE,
        file="",
    )
    assert all(res)


def test_unsuccessful_update_bad_endpoint(client):
    client.connect()
    res = client.update(
        endpoint="bad_campaigns",
        id=os.getenv("CAMPAIGN_ID"),
        budget_value=BUDGET_VALUE,
        file="",
    )
    assert not all(res)


def test_unsuccessful_update_bad_campaign(client):
    client.connect()
    res = client.update(
        endpoint="campaigns",
        id="bad-id",
        budget_value=BUDGET_VALUE,
        file="",
    )
    assert not all(res)
