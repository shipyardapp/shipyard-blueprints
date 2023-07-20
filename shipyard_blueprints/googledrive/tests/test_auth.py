import os
from shipyard_googledrive import GoogleDriveClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def conn_helper(client: GoogleDriveClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        print('Could not connect to google drive')
        return 1

def test_good_connection():
    creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    client = GoogleDriveClient(creds)
    assert conn_helper(client) == 0



def test_bad_connection():
    creds = 'bad_creds'
    client = GoogleDriveClient(creds)
    assert conn_helper(client) == 1

