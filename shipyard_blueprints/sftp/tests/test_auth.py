import os

from dotenv import load_dotenv, find_dotenv

from shipyard_sftp.sftp import SftpClient


load_dotenv(find_dotenv())


def conn_helper(client: SftpClient) -> int:
    print(
        f"Connecting to {client.host} as {client.user} with {client.pwd} on port {client.port}"
    )

    return client.connect()


def test_good_connection():
    host = os.getenv("SFTP_HOST")
    user = os.getenv("SFTP_USERNAME")
    pwd = os.getenv("SFTP_PASSWORD")
    port = os.getenv("SFTP_PORT")
    client = SftpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 0


def test_bad_user():
    host = os.getenv("SFTP_HOST")
    user = "bad_user"
    pwd = os.getenv("SFTP_PASSWORD")
    port = os.getenv("SFTP_PORT")
    client = SftpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_pwd():
    host = os.getenv("SFTP_HOST")
    user = os.getenv("SFTP_USER")
    pwd = "bad_pwd"
    port = os.getenv("SFTP_PORT")
    client = SftpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_host():
    host = "bad_host"
    user = os.getenv("SFTP_USER")
    pwd = os.getenv("SFTP_PASSWORD")
    port = os.getenv("SFTP_PORT")
    client = SftpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1


def test_bad_port():
    host = os.getenv("SFTP_HOST")
    user = os.getenv("SFTP_USER")
    pwd = os.getenv("SFTP_PASSWORD")
    port = "1111"
    client = SftpClient(user=user, pwd=pwd, host=host, port=port)
    assert conn_helper(client) == 1
