import snowflake.connector
from snowflake.connector.errors import DatabaseError, ForbiddenError, ProgrammingError
from shipyard_templates import Database


class SnowflakeClient(Database):
    def __init__(self, username, pwd, database=None, account=None, warehouse=None, schema=None) -> None:
        self.username = username
        self.pwd = pwd
        self.account = account
        self.warehouse = warehouse
        self.schema = schema
        self.database = database
        super().__init__(username, pwd, account=account,
                         warehouse=warehouse, schema=schema, database=database)

    def connect(self):
        try:
            con = snowflake.connector.connect(user=self.username, password=self.pwd,
                                              account=self.account, warehouse=self.warehouse,
                                              database=self.database, schema=self.schema)
            self.logger.info(f"Successfully connected to snowflake")
            return con
        except Exception as e:
            # self.EXIT_CODE_INVALID_CREDENTIALS
            self.logger.error(
                f"Could not authenticate to account {self.account} for user {self.username}")
            return 1  # failed connection

    def upload_csv_to_table(self, file: str):
        pass

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass
