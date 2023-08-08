import os
from shipyard_dbt import DbtClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def conn_helper(client: DbtClient):
    conn = client.connect()
    if conn == 0:
        return 0
    else:
        client.logger.error("Could not connect to DBT")
        return 1


def test_good_connection():
    dbt = DbtClient(
        access_token=os.getenv("DBT_API_KEY"), account_id=os.getenv("DBT_ACCOUNT_ID")
    )
    assert dbt.connect() == 0


def test_bad_token():
    dbt = DbtClient(access_token="bad_token", account_id=os.getenv("DBT_ACCOUNT_ID"))
    assert conn_helper(dbt) == 1


def test_bad_account():
    dbt = DbtClient(access_token=os.getenv("DBT_API_KEY"), account_id="bad_account")
    assert conn_helper(dbt) == 1
