from shipyard_templates import CloudStorage
import databricks_cli
from databricks_cli.sdk.api_client import ApiClient
from databricks.sdk import WorkspaceClient


class DatabricksClient(CloudStorage):
    def __init__(self, access_token: str, instance_url: str) -> None:
        self.access_token = access_token
        self.instance_url = instance_url
        self.base_url = f"https://{self.instance_url}/api/2.0"
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        # self.headers = {
        #     'Authorization': f"Bearer {self.access_token}",
        #     'Content-Type': 'application/json'
        # }
        super().__init__(access_token=access_token, instance_url=instance_url)

    def connect(self):
        # client = ApiClient(host=self.base_url, token=self.access_token)
        client = WorkspaceClient(host=self.base_url, token=self.access_token)

        return client

    def upload_files(self):
        pass

    def remove_files(self):
        pass

    def move_or_rename_files(self):
        pass

    def download_files(self):
        pass
