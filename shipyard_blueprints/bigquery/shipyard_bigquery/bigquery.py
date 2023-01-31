from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from templates.database import GoogleDatabase


class BigQueryClient(GoogleDatabase):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account)

    def connect(self):
        client = bigquery.Client(credentials=self.service_account)
        return client

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass
