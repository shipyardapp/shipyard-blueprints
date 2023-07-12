from json import load
import os
from shipyard_googlecloud import GoogleCloudClient
from dotenv import load_dotenv, find_dotenv

# load the local environment variables
load_dotenv(find_dotenv())

def conn_helper(client:GoogleCloudClient):
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.exception(e)
        return 1


def test_good_connection():
    creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    client = GoogleCloudClient(creds)
    assert conn_helper(client) == 0

def test_bad_connection():
    creds = os.environ.get('BAD_CREDENTIALS')
    client = GoogleCloudClient(creds)
    assert conn_helper(client) == 1


