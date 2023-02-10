from templates.cloudstorage import CloudStorage
from boxsdk import Client, JWTAuth
import os
import json


class BoxClient(CloudStorage):
    def __init__(self, service_account: str) -> None:
        self.service_account = service_account
        super().__init__(service_account=service_account)

    def connect(self):
        if os.path.isfile(self.service_account):
            auth = JWTAuth.from_settings_file(self.service_account)
        else:
            service_dict = json.loads(self.service_account, strict=False)
            auth = JWTAuth.from_settings_dictionary(service_dict)
        client = Client(auth)
        client.user().get()
        return client

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass
