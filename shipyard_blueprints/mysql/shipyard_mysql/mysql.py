import sys
from sqlalchemy import create_engine, text
from shipyard_templates import Database, ExitCodeException
from sqlalchemy.engine import url


class MySqlClient(Database):
    def __init__(
        self,
        user: str,
        pwd: str,
        host: str,
        database: str,
        port: int = 3306,
        url_params=None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params
        super().__init__(
            user=user,
            pwd=pwd,
            host=host,
            database=database,
            port=port,
            url_params=url_params,
        )

    def connect(self):
        con_str = f"mysql+mysqlconnector://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}"
        try:
            return create_engine(con_str).connect()
        except Exception as e:
            self.logger.error(
                f"Could not connect to {self.host} with the provided credentials"
            )
            raise ExitCodeException(e, self.EXIT_CODE_INVALID_CREDENTIALS)

    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass

    def upload(self, file: str):
        pass
