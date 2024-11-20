import pytest
import os

from dotenv import load_dotenv, find_dotenv
from shipyard_magnite import MagniteClient
from shipyard_magnite.errs import ReadError
from pytest import MonkeyPatch
from copy import deepcopy
from requests.exceptions import HTTPError

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def client():
    user = os.getenv("MAGNITE_USERNAME")
    pwd = os.getenv("MAGNITE_PASSWORD")
    return MagniteClient(user, pwd)


def test_read_successful_campaign(client):
    campaign_id = os.getenv("CAMPAIGN_ID")
    client.connect()
    results = client.read("campaigns", campaign_id)
    assert results is not None
    assert len(results) > 0


def test_read_invalid_campaign(client):
    campaign_id = "bad_campaign_id"
    client.connect()

    with pytest.raises(ReadError):
        client.read("campaigns", campaign_id)


def test_read_invalid_endpoint(client):
    campaign_id = os.getenv("CAMPAIGN_ID")
    client.connect()
    with pytest.raises(ReadError):
        client.read("bad-endpoint", campaign_id)
