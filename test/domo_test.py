from shipyard_blueprints import DomoClient
from settings import Domo

client_id = Domo.CLIENT_ID
secret = Domo.SECRET


def test_connection():
    client = DomoClient(client_id, secret)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
