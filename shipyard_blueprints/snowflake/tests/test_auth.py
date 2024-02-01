import os
import pytest
from shipyard_snowflake import SnowflakeClient
from dotenv import load_dotenv, find_dotenv
from shipyard_snowflake import SnowparkClient
from shipyard_templates import ShipyardLogger
from typing import Union

if env_exists := os.path.exists(".env"):
    load_dotenv()

logger = ShipyardLogger.get_logger()


def conn_helper(client: Union[SnowflakeClient, SnowparkClient]) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        logger.error("Could not connect to Snowflake")
        logger.exception(e)
        return 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_good_connection():
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    client = SnowflakeClient(username=user, password=pwd, account=account)

    assert conn_helper(client) == 0


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_bad_user():
    user = "bad_user"
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    client = SnowflakeClient(username=user, password=pwd, account=account)

    assert conn_helper(client) == 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_bad_pwd():
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = "bad_password"
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    client = SnowflakeClient(username=user, password=pwd, account=account)

    assert conn_helper(client) == 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_bad_account():
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = "bad_account"

    client = SnowflakeClient(username=user, password=pwd, account=account)

    assert conn_helper(client) == 1


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_snowpark_connection():
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    db = os.getenv("SNOWFLAKE_DATABASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    client = SnowparkClient(
        username=user,
        pwd=pwd,
        account=account,
        schema=schema,
        database=db,
        warehouse=warehouse,
    )
    assert conn_helper(client) == 0
