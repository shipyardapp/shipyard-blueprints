from sqlalchemy import create_engine, text
from shipyard_templates import Database


class SqlServerClient(Database):
    def __init__(self, user: str, pwd: str, host: str = None, database: str = None, port: int = 1433, url_params=None) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.database = database
        self.port = port
        self.url_params = url_params
        super().__init__(user, pwd, host=host, database=database,
                       port=port, url_params=url_params)

    def connect(self):
        # con_str = f'mssql+pymssql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}'
        con_str = f'mssql+pyodbc://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?driver=SQL Server?{self.url_params}'
        return create_engine(con_str).connect()

    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass

    def upload(self, file: str):
        pass
