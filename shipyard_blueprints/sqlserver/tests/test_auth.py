import os
import pytest
from dotenv import load_dotenv, find_dotenv
from shipyard_sqlserver import SqlServerClient
from shipyard_sqlserver.exceptions import SqlServerConnectionError
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()

load_dotenv(find_dotenv())


@pytest.fixture(scope="module")
def creds():
    return {
        "host": os.getenv("MSSQL_HOST"),
        "pwd": os.getenv("MSSQL_PASSWORD"),
        "user": os.getenv("MSSQL_USERNAME"),
        "db": os.getenv("MSSQL_DATABASE"),
        "port": os.getenv("MSSQL_PORT"),
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


def test_auth_bad_user(creds):
    client = SqlServerClient(
        user="bad_user", pwd=creds["pwd"], host=creds["host"], database=creds["db"]
    )

    assert conn_helper(client) == 1


def test_auth_bad_pwd(creds):
    client = SqlServerClient(
        user=creds["user"], pwd="bad_pwd", host=creds["host"], database=creds["db"]
    )
    assert conn_helper(client) == 1


def test_raises_credential_exception_bad_pwd(creds):
    client = SqlServerClient(
        user=creds["user"], pwd="bad_pwd", host=creds["host"], database=creds["db"]
    )
    with pytest.raises(SqlServerConnectionError):
        client.connect()


def test_raises_credential_exception_bad_user(creds):
    client = SqlServerClient(
        user="bad_user", pwd=creds["pwd"], host=creds["host"], database=creds["db"]
    )
    with pytest.raises(SqlServerConnectionError):
        client.connect()


def test_raises_credential_exception_bad_host(creds):
    client = SqlServerClient(
        user=creds["user"], pwd=creds["pwd"], host="123456789", database=creds["db"]
    )
    with pytest.raises(SqlServerConnectionError):
        client.connect()
