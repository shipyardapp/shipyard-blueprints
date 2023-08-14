from shipyard_airbyte import AirbyteClient
from settings import AirbyteConfig as ac

token = ac.TOKEN
connection_id = ac.CONNECTION_ID

client = AirbyteClient(token)


def test_trigger_sync():
    resp = client._trigger_sync_response(connection_id=connection_id)
    assert resp.status_code == 200


def test_trigger_bad_sync():
    resp = client._trigger_sync_response(connection_id=connection_id)
    assert resp.status_code != 200
