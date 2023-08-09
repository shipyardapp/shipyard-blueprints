import os
from shipyard_blueprints import AirbyteClient


def test_connection():
    token = os.getenv("AIRBYTE_API_TOKEN")
    client = AirbyteClient(token)
    conn = client.connect()
    assert conn == 200
