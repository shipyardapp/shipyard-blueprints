from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from shipyard_templates import GoogleDatabase
import json
import os
import tempfile


class BigQueryClient(GoogleDatabase):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account)

    def _set_env_vars(self):
        try:
            json_credentials = json.loads(self.service_account)
            fd, path = tempfile.mkstemp()
            print(f"Storing json credentials temporarily at {path}")
            with os.fdopen(fd, "w") as tmp:
                tmp.write(self.service_account)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
            return path
        except Exception as e:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.service_account

    def connect(self):
        fd = self._set_env_vars()
        client = bigquery.Client()
        return client

    def execute_query(self, query: str):
        pass

    def fetch_query_results(self, query: str):
        pass

    def upload_csv_to_table(self, file: str):
        pass
