from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from shipyard_templates import CloudStorage
import json
import tempfile
import os


class GoogleDriveClient(CloudStorage):
    SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=service_account)

    def _set_env_vars(self):
        try:
            json_credentials = json.loads(self.service_account)
            fd, path = tempfile.mkstemp()
            print(f'Storing json credentials temporarily at {path}')
            with os.fdopen(fd, 'w') as tmp:
                tmp.write(self.service_account)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
            return path
        except Exception as e:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.service_account

    def connect(self):
        tmp_path = self._set_env_vars()
        credentials = service_account.Credentials.from_service_account_file(
            tmp_path, scopes=self.SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        return service

    def upload_files(self):
        pass

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass

    def download_files(self):
        pass
