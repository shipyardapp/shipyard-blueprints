# from sqlalchemy import create_engine, text
# from sqlalchemy.engine.url import URL
import redshift_connector
from shipyard_templates import Database


class RedshiftClient(Database):
    def __init__(
        self,
        user: str,
        pwd: str,
        host: str = None,
        database: str = None,
        port: str = None,
        url_params: str = None,
    ) -> None:
        self.user = user
        self.pwd = pwd
        self.host = host
        self.database = database
        self.port = port
        self.url_params = url_params
        super().__init__(
            user, pwd, host=host, database=database, port=port, url_params=url_params
        )

    def connect(self):
        # con_str = URL.create(drivername= 'redshift+redshift_connector', host = self.host, password= self.password, username= self.user, port = self.port, database= self.database)
        conn = redshift_connector.connect(
            host=self.host, database=self.database, user=self.user, password=self.pwd
        )
        return conn

    def execute_query(self, query: str):
        pass

    def fetch(self, query: str):
        pass

    def upload(self, file: str):
        pass
