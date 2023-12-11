import os
import json
import tempfile


from googleapiclient.discovery import build
from google.oauth2 import service_account
from shipyard_templates import Spreadsheets


class GoogleSheetsClient(Spreadsheets):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=service_account)

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
        creds = service_account.Credentials.from_service_account_file(fd)
        service = build("sheets", "v4", credentials=creds)
        drive_service = build("drive", "v3", credentials=creds)
        return service, drive_service

    def fetch(self):
        pass

    def upload(self):
        pass
