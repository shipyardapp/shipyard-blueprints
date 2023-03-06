from shipyard_blueprints import SftpClient
from settings import SFTP

host = SFTP.HOST
user = SFTP.USER
pwd = SFTP.PWD
port = SFTP.PORT


def test_connection():
    client = SftpClient(host=host, port=port, user=user, key='rsa_id')

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
