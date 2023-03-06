from shipyard_blueprints import HexClient
from settings import Hex

token = Hex.API_TOKEN
project_id = Hex.PROJECT_ID


def test_connection():
    client = HexClient(token, project_id)

    def connection_test():
        try:
            conn = client.connect()
            code = conn.status_code
            return code
        except Exception as e:
            return 1
    assert connection_test() == 200
