from shipyard_blueprints import LookerClient
from settings import Looker

client_id = Looker.CLIENT_ID
secret = Looker.SECRET
base_url = "https://shipyard.cloud.looker.com/"


def test_connection():
    client = LookerClient(base_url=base_url, client_id=client_id, client_secret=secret)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
