from templates.database import Database
from sqlalchemy import create_engine, text
import sys


class MySqlClient(Database):

    def __init__(self, user: str, pwd: str, host: str, port: int, database: str, url_params=None) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params

    def connect(self):
        con_str = f'mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}'
        try:
            engine = create_engine(con_str)
            return engine
        except Exception as e:
            self.logger.error(
                f"Could not connect to {self.host} with the provided credentials")
            return 1

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass
