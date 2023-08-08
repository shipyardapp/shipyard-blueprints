from shipyard_blueprints import GoogleDriveClient
from settings import GoogleDrive

service_account = GoogleDrive.SERVICE_ACCOUNT


def test_connection():
    client = GoogleDriveClient(service_account)

    def connection_helper():
        try:
            conn = client.connect()
            assert conn is not None
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
