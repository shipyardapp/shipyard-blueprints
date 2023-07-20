import os
from shipyard_mysql import MySqlClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def conn_helper(client:MySqlClient) -> int:
    try:
        client.connect()
        return 0
    except Exception as e:
        print("Could not connect to MySQL")
        print(e)
        return 1
    else:
        print("Could not connect to MySQL")
        return 1

def test_good_connection():
    user = os.getenv('MYSQL_USERNAME')
    pwd = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db = os.getenv('MYSQL_DATABASE')
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db)
    print(f" port is {client.port}")
    assert conn_helper(client) == 0

def test_good_connection_with_port():
    user = os.getenv('MYSQL_USERNAME')
    pwd = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db = os.getenv('MYSQL_DATABASE')
    port = os.getenv('MYSQL_PORT')
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db, port = port)
    print(f"port is {client.port}")
    assert conn_helper(client) == 0

def test_bad_user():
    user = 'bad_user'
    pwd = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db = os.getenv('MYSQL_DATABASE')
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db)
    assert conn_helper(client) == 1

def test_bad_pwd():
    user = os.getenv('MYSQL_USERNAME')
    pwd = 'bad_pwd'
    host = os.getenv('MYSQL_HOST')
    db = os.getenv('MYSQL_DATABASE')
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db)
    assert conn_helper(client) == 1

def test_bad_host():
    user = os.getenv('MYSQL_USERNAME')
    pwd = os.getenv('MYSQL_PASSWORD')
    host = 'bad_host'
    db = os.getenv('MYSQL_DATABASE')
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db)
    assert conn_helper(client) == 1

def test_bad_db():
    user = os.getenv('MYSQL_USERNAME')
    pwd = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db = 'bad_db'
    client = MySqlClient(user = user, pwd = pwd, host = host, database = db)
    assert conn_helper(client) == 1


