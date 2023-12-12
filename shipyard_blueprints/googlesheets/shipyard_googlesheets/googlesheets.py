import json

from google.oauth2 import service_account
from shipyard_templates import Spreadsheets


class GoogleSheetsClient(Spreadsheets):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=service_account)

    def connect(self):
        service_account_str = self.service_account.replace("\n", "\\n")
        service_account.Credentials.from_service_account_info(
            json.loads(service_account_str)
        )

    def fetch(self):
        pass

    def upload(self):
        pass
