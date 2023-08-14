from shipyard_templates import CloudStorage
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core import exceptions


class AzureBlobClient(CloudStorage):
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        super().__init__(connection_string=connection_string)

    def connect(self) -> BlobServiceClient:
        client = BlobServiceClient.from_connection_string(
            conn_str=self.connection_string
        )
        return client

    def move_or_rename_files(self):
        pass

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def remove_files(self):
        pass
