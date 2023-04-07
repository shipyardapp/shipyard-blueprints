from shipyard_airbyte import AirbyteClient
from settings import AirbyteConfig as ac 

token = ac.TOKEN 
connection_id = ac.CONNECTION_ID 

client = AirbyteClient(token, connection_id)

def test_trigger_sync():
    resp = client.trigger_sync(check_status=False)
    assert resp.status_code == 200



