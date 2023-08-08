from shipyard_blueprints import TableauClient
from settings import Tableau

user = Tableau.USER
pwd = Tableau.PWD
token = Tableau.ACCESS_TOKEN
token_name = Tableau.ACCESS_TOKEN_NAME
server_url = Tableau.SERVER
site_id = Tableau.SITE


def test_connection():
    client = TableauClient(
        username=user, password=pwd, server_url=server_url, site=site_id
    )

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1

    assert connection_helper() == 0
