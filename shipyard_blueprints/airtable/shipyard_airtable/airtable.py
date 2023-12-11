import requests
from shipyard_templates import Spreadsheets


class AirtableClient(Spreadsheets):
    def __init__(
        self, api_key: str, base_id: str = None, table_name: str = None
    ) -> None:
        self.base_id = base_id
        self.api_key = api_key
        self.table_name = table_name
        super().__init__(api_key=api_key, base_id=base_id, table_name=table_name)

    def connect(self):
        url = "https://api.airtable.com/v0/meta/whoami"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(url, headers=headers)
        return response.status_code

    def fetch(self):
        pass

    def upload(self):
        pass
