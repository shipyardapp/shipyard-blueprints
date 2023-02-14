from shipyard_blueprints import ModeClient
from settings import Mode

api_key = Mode.API_KEY
api_secret = Mode.API_SECRET
account = Mode.ACCOUNT


def test_connection():
    client = ModeClient(api_token=api_key,
                        api_secret=api_secret, account=account)

    def connection_helper():
        try:
            conn = client.connect()
            assert conn.status_code == 200

            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
