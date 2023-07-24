import os
from shipyard_snowflake import SnowflakeClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def conn_helper(client: SnowflakeClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error("Could not connect to Snowflake")
        client.logger.exception(e)
        return 1
    else:
        client.logger.error("Could not connect to Snowflake")
        return 1


def test_good_connection():
    user = os.getenv('SNOWFLAKE_USERNAME')
    pwd = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')

    client = SnowflakeClient(username = user, pwd = pwd, account = account)

    assert conn_helper(client) == 0

def test_bad_user():
    user = 'bad_user'
    pwd = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv('SNOWFLAKE_ACCOUNT')

    client = SnowflakeClient(username = user, pwd = pwd, account = account)

    assert conn_helper(client) == 1


def test_bad_pwd():
    user = os.getenv('SNOWFLAKE_USERNAME')
    pwd = 'bad_password'
    account = os.getenv('SNOWFLAKE_ACCOUNT')

    client = SnowflakeClient(username = user, pwd = pwd, account = account)

    assert conn_helper(client) == 1



def test_bad_account():
    user = os.getenv('SNOWFLAKE_USERNAME')
    pwd = os.getenv('SNOWFLAKE_PASSWORD')
    account = 'bad_account'

    client = SnowflakeClient(username = user, pwd = pwd, account = account)

    assert conn_helper(client) == 1


