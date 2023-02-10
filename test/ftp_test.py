from shipyard_blueprints import FtpClient
from settings import Ftp

host = Ftp.HOST
user = Ftp.USER
pwd = Ftp.PWD
port = Ftp.PORT


def test_connection():
    client = FtpClient(host, user, pwd, port)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
