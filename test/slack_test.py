from shipyard_blueprints import SlackClient
from settings import Slack

token = Slack.TOKEN


def test_connection():
    client = SlackClient(token)

    def connection_helper():
        try:
            conn = client.connect()
            assert conn is not None
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
