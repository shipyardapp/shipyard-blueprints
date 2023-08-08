from shipyard_blueprints import AthenaClient
from settings import Athena

access_key = Athena.AWS_ACCESS_KEY
secret_key = Athena.AWS_SECRET_KEY
region = Athena.AWS_REGION


def test_connection():
    client = AthenaClient(user=access_key, pwd=secret_key, region=region)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
