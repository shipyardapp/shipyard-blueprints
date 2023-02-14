from shipyard_blueprints import GoogleSheetsClient
from settings import GoogleSheets

service_account = GoogleSheets.SERVICE_ACCOUNT


def test_connection():
    client = GoogleSheetsClient(service_account)

    def connection_helper():
        try:
            service, drive = client.connect()
            assert service is not None and drive is not None
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
