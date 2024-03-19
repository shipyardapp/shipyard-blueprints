from shipyard_templates import Database
from sqlalchemy import create_engine
from typing import Optional


class PostgresqlClient(Database):
    def __init__(
        self,
        user: str,
        pwd: str,
        host: str,
        port: int = 5432,
        database: Optional[str] = None,
        url_params=None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database = database
        self.url_params = url_params
        self._conn = None
        super().__init__(
            user, pwd, host=host, port=port, database=database, url_params=url_params
        )

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.connect()
        return self._conn

    def connect(self):
        con_str = f"postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?{self.url_params}"
        return create_engine(con_str).connect()

    def execute_query(self, query: str):
        pass

    def upload(self, file: str):
        pass

    def fetch(self, query: str):
        pass
