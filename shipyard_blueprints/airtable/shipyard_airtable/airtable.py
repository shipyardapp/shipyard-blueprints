from templates.spreadsheets import Spreadsheets
from pyairtable import Table


class AirtableClient(Spreadsheets):
    def __init__(self, base_id: str, api_key: str, table_name: str) -> None:
        self.base_id = base_id
        self.api_key = api_key
        self.table_name = table_name
        super().__init__(api_key=api_key, base_id=base_id, table_name=table_name)

    def connect(self):
        return Table(self.api_key, self.base_id, self.table_name)
