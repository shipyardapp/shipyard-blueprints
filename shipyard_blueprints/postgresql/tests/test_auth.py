import os
from shipyard_postgresql import PostgresqlClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def conn_helper(client: PostgresqlClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error('Could not connect to Postgres')
        return 1
    else:
        client.logger.error('Could not connect to Postgres')
        return 1

def test_good_connection():
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USERNAME')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DATABASE')
    port = os.getenv('POSTGRES_PORT')
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 0


def test_bad_host():
    host = 'bad_host'
    user = os.getenv('POSTGRES_USERNAME')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DATABASE')
    port = os.getenv('POSTGRES_PORT')
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1

def test_bad_user():
    host = os.getenv('POSTGRES_HOST')
    user = 'bad_user'
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DATABASE')
    port = os.getenv('POSTGRES_PORT')
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1

def test_bad_pwd():
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USERNAME')
    pwd = 'bad_password'
    db = os.getenv('POSTGRES_DATABASE')
    port = os.getenv('POSTGRES_PORT')
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1

def test_bad_db():
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USERNAME')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = 'bad_db'
    port = os.getenv('POSTGRES_PORT')
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1

def test_bad_port():
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USERNAME')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DATABASE')
    port = 'bad_port'
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1


def test_diff_port():
    host = os.getenv('POSTGRES_HOST')
    user = os.getenv('POSTGRES_USERNAME')
    pwd = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DATABASE')
    port = 1234
    client = PostgresqlClient(user = user, pwd = pwd, host = host, port = port, database = db)
    assert conn_helper(client) == 1


