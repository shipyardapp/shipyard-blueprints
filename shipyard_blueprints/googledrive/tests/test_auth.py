import os
import pytest
from shipyard_googledrive import GoogleDriveClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

if env_exists := os.path.exists(".env"):
    load_dotenv()


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def conn_helper(client: GoogleDriveClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        print("Could not connect to google drive")
        return 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_good_connection():
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = GoogleDriveClient(creds)
    assert conn_helper(client) == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_bad_connection():
    creds = "bad_creds"
    client = GoogleDriveClient(creds)
    assert conn_helper(client) == 1
