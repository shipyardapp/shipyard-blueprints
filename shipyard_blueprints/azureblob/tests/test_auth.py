import os
from shipyard_azureblob import AzureBlobClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def conn_helper(client: AzureBlobClient) -> int:
    """Helper function that returns 0 with a successful connection and 1 otherwise"""
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error("Could not connect to azure")
        return 1
    else:
        return 1


def test_good_connection():
    """Test that a good connection works"""
    con_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    client = AzureBlobClient(connection_string=con_str)
    assert conn_helper(client) == 0


def test_bad_connnection():
    con_str = os.getenv("BAD_STORAGE_CONNECTION")
    client = AzureBlobClient(connection_string=con_str)
    assert conn_helper(client) == 1
