import os
from shipyard_box import BoxClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def conn_helper(client:BoxClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error("Could not connect to Box")
        return 1


def test_good_connection():
    creds = os.getenv("BOX_APPLICATION_CREDENTIALS")
    client = BoxClient(service_account=creds)
    assert conn_helper(client) == 0

def test_bad_connetion():
    creds = os.getenv("BAD_CREDENTIALS")
    client = BoxClient(service_account=creds)
    assert conn_helper(client) == 1

