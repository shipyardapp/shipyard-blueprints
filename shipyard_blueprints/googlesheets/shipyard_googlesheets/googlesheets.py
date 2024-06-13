import os
import tempfile

from google.oauth2 import service_account
from googleapiclient.discovery import build
from shipyard_templates import Spreadsheets


class GoogleSheetsClient(Spreadsheets):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account

    def _set_env_vars(self):
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "w") as tmp:
            tmp.write(self.service_account)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        return path

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
