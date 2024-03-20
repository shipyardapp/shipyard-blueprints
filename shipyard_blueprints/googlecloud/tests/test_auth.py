import os

from dotenv import load_dotenv, find_dotenv

from shipyard_googlecloud import GoogleCloudClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger().get_logger()

# load the local environment variables
load_dotenv(find_dotenv(raise_error_if_not_found=True))


def conn_helper(client: GoogleCloudClient):
    try:
        client.connect()
        return 0
    except Exception as e:
        logger.authtest(e)
        return 1


def test_good_connection():
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    client = GoogleCloudClient(creds)
    assert conn_helper(client) == 0


def test_bad_connection():
    creds = os.environ.get("BAD_CREDENTIALS")
    client = GoogleCloudClient(creds)
    assert conn_helper(client) == 1
