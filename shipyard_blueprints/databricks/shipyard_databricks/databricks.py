from templates.cloudstorage import CloudStorage
import databricks_cli
from databricks_cli.sdk.api_client import ApiClient


class DatabricksClient(CloudStorage):
    def __init__(self, access_token: str, instance_url: str) -> None:
        self.access_token = access_token
        self.instance_url = instance_url
        # self.headers = {
        #     'Authorization': f"Bearer {self.access_token}",
        #     'Content-Type': 'application/json'
        # }
        super().__init__(access_token=access_token, instance_url=instance_url)

    def connect(self):
        client = ApiClient(host=self.instance_url, token=self.access_token)
        return client

    def upload_files(self):
        pass

    def remove_files(self):
        pass

    def move_or_rename_files(self):
        pass

    def download_files(self):
        pass
