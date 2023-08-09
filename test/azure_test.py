from shipyard_blueprints import AzureBlobClient
from settings import AzureBlob

connection_str = AzureBlob.AZURE_CONNECTION_STRING


def test_connection():
    client = AzureBlobClient(connection_str)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
