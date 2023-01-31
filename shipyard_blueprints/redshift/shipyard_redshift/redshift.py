from sqlalchemy import create_engine, text
from templates.database import Database


class RedshiftClient(Database):
    def __init__(self, user: str, pwd: str, host: str = None, database: str = None, port: str = None, url_params: str = None) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.database = database
        self.port = port
        self.url_params = url_params
        super().__init__(user, pwd, host=host, database=database,
                         port=port, url_params=url_params)

    def connect(self):
        con_str = f'postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_parameters}'
        engine = create_engine(con_str)
        return engine

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass
