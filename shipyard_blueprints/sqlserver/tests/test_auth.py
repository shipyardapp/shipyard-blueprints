import os
from shipyard_sqlserver import SqlServerClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def conn_helper(client: SqlServerClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error("Could not connect to sql server")
        client.logger.exception(e)
        return 1
    else:
        client.logger.error("Could not connect to sql server")
        return 1


def test_good_connection():
    user = os.getenv("MSSQL_USERNAME")
    host = os.getenv("MSSQL_HOST")
    pwd = os.getenv("MSSQL_PASSWORD")
    db = os.getenv("MSSQL_DATABASE")
    client = SqlServerClient(user=user, pwd=pwd, host=host, database=db)
    assert conn_helper(client) == 0


def test_bad_user():
    pass
