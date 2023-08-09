import os
from shipyard_ftp import FtpClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def conn_helper(client: FtpClient):
    try:
        conn = client.connect()
        return 0
    except Exception as e:
        print(e)
        print("Could not connect to ftp server")
        return 1


def test_good_connection():
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASSWORD")
    host = os.getenv("FTP_HOST")
    port = os.getenv("FTP_PORT")
    client = FtpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 0


def test_bad_connection():
    user = "bad_user"
    pwd = "bad_pwd"
    host = "bad_host"
    client = FtpClient(user=user, pwd=pwd, host=host)
    assert conn_helper(client) == 1


def test_bad_user():
    user = "bad_user"
    pwd = os.getenv("FTP_PASSWORD")
    host = os.getenv("FTP_HOST")
    port = os.getenv("FTP_PORT")
    client = FtpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_pwd():
    user = os.getenv("FTP_USER")
    pwd = "bad_pwd"
    host = os.getenv("FTP_HOST")
    port = os.getenv("FTP_PORT")
    client = FtpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_host():
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASSWORD")
    host = "bad_host"
    port = os.getenv("FTP_PORT")
    client = FtpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_port():
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASSWORD")
    host = os.getenv("FTP_HOST")
    port = 50
    client = FtpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_no_port():
    user = os.getenv("FTP_USER")
    pwd = os.getenv("FTP_PASSWORD")
    host = os.getenv("FTP_HOST")
    client = FtpClient(user=user, pwd=pwd, host=host)
    assert conn_helper(client) == 0
