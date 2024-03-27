import os
import pytest
from shipyard_postgresql import PostgresClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

single_file = "test.csv"
dest_file = "download.csv"
nested_file = "mult_1.csv"
dest_folder = "newdir"
regex_file = "mult"
regex_folder = "mult"


def conn_helper(client: PostgresClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        return 1


def test_good_connection():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USERNAME")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DATABASE")
    port = os.getenv("POSTGRES_PORT")
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 0


def test_bad_host():
    host = "bad_host"
    user = os.getenv("POSTGRES_USERNAME")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DATABASE")
    port = os.getenv("POSTGRES_PORT")
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1


def test_bad_user():
    host = os.getenv("POSTGRES_HOST")
    user = "bad_user"
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DATABASE")
    port = os.getenv("POSTGRES_PORT")
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1


def test_bad_pwd():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USERNAME")
    pwd = "bad_password"
    db = os.getenv("POSTGRES_DATABASE")
    port = os.getenv("POSTGRES_PORT")
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1


def test_bad_db():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USERNAME")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = "bad_db"
    port = os.getenv("POSTGRES_PORT")
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1


def test_bad_port():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USERNAME")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DATABASE")
    port = "bad_port"
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1


def test_diff_port():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USERNAME")
    pwd = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DATABASE")
    port = 1234
    client = PostgresClient(user=user, pwd=pwd, host=host, port=port, database=db)
    assert conn_helper(client) == 1
