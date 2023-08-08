from google.cloud import storage
from shipyard_templates import CloudStorage
import json
import os
import tempfile


class GoogleCloudClient(CloudStorage):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=self.service_account)

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
        return storage.Client()

    def move_or_rename_files(self):
        pass

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def remove_files(self):
        pass
