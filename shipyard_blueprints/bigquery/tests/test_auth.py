import os
from shipyard_bigquery import BigQueryClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def conn_helper(client: BigQueryClient) -> int:
    try:
        conn = client.connect()
        return 0
    except Exception as e:
        print("Could not connect to BigQuery")
        return 1


def test_good_connection():
    client = BigQueryClient(service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    assert conn_helper(client) == 0


def test_bad_connection():
    client = BigQueryClient("bad_credentials_here")
    assert conn_helper(client) == 1


def test_email():
    client = BigQueryClient(service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    email = os.getenv("EMAIL")
    assert client.email == email
