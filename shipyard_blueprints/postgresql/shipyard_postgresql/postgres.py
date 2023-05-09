from shipyard_templates import Database
from sqlalchemy import create_engine


class PostgresqlClient(Database):
    def __init__(self, user: str, pwd: str, host: str = None, port: int = None, database: str = None, url_params=None) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params
        super().__init__(user, pwd, host=host, port=port,
                         database=database, url_params=url_params)

    def connect(self):
        con_str = f'postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}'
        engine = create_engine(con_str)
        return engine

    def execute_query(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass

    def fetch_query_results(self, query: str):
        pass
