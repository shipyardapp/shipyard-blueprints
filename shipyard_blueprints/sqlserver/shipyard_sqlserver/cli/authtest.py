import os
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_sqlserver import SqlServerClient
from shipyard_sqlserver.errors.exceptions import SqlServerConnectionError
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("SQL_HOST"),
        "pwd": os.getenv("SQL_PWD"),
        "user": os.getenv("SQL_USER"),
        "db": os.getenv("SQL_DB"),
    }


def conn_helper(client: SqlServerClient):
    try:
        client.connect()
        logger.info("Successfully connected to SQL Server")
        return 0
    except SqlServerConnectionError:
        return 1
    except Exception as e:
        return 1


def test_auth(creds):
    client = SqlServerClient(
        user=creds["user"], pwd=creds["pwd"], host=creds["host"], database=creds["db"]
    )
    assert conn_helper(client) == 0


def test_auth_bad():
    pass
