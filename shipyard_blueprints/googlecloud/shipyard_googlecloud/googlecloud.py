from google.cloud import storage
from templates.cloudstorage import CloudStorage


class GoogleCloudClient(CloudStorage):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=service_account)

    def connect(self):
        client = storage.Client(credentials=self.service_account)
        return client

    def move_or_rename_files(self):
        pass

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def remove_files(self):
        pass
