from shipyard_blueprints import BoxClient
from settings import Box

service_account = Box.SERVICE_ACCOUNT

client = BoxClient(service_account)


def test_connection():
    def connection_helper():
        try:
            client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
