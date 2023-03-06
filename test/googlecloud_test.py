from shipyard_blueprints import GoogleCloudClient
from settings import GoogleCloud

service_account = GoogleCloud.SERVICE_ACCOUNT


def test_connection():
    client = GoogleCloudClient(service_account)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
