from shipyard_blueprints import SnowflakeClient
from settings import Snowflake as snow


snowflake = SnowflakeClient(username=snow.USER, database=snow.DATABASE, pwd=snow.PWD,
                            account=snow.ACCOUNT, warehouse=snow.WAREHOUSE, schema=snow.SCHEMA)


def test_connection():
    def connection_helper():
        try:
            conn = snowflake.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0
