from shipyard_blueprints import SnowflakeClient
import unittest
import os


user = os.getenv("SNOWFLAKE_USER")
pwd = os.getenv("SNOWFLAKE_PWD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
schema = os.getenv("SNOWFLAKE_SCHEMA") 
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")


class ConnectionTest(unittest.TestCase):
    def test_connection(self):
        snowflake = SnowflakeClient(
            username=user, pwd=pwd, database=database, account=account, warehouse=warehouse, schema=schema)
        con = snowflake.connect()
        self.assertNotEqual(con, 1)


unittest.main()
