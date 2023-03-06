from shipyard_blueprints import PostgresqlClient
from settings import Postgres

host = Postgres.HOST
database = Postgres.DATABASE
user = Postgres.USER
pwd = Postgres.PWD
port = Postgres.PORT


def test_connection():
    client = PostgresqlClient(
        user=user, pwd=pwd, host=host, port=port, database=database)

    def connection_helper():
        try:
            conn = client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
