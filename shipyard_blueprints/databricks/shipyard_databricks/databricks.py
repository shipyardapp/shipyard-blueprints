import databricks
from shipyard_templates import CloudStorage
from databricks.sdk import WorkspaceClient


class DatabricksClient(CloudStorage):
    def __init__(self, access_token: str, instance_url: str) -> None:
        self.access_token = access_token
        self.instance_url = instance_url
        self.base_url = f"https://{self.instance_url}/api/2.0"
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        super().__init__(access_token=access_token, instance_url=instance_url)

    def connect(self):
        client = WorkspaceClient(host=self.base_url, token=self.access_token)

        return client

    def upload(self):
        pass

    def remove(self):
        pass

    def move(self):
        pass

    def download(self):
        pass
