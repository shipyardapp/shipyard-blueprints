from shipyard_templates import CloudStorage
from dropbox import Dropbox

class DropboxClient(CloudStorage):
    def __init__(self, access_key:str) -> None:
        self.access_key = access_key
        super().__init__(access_key = access_key)

    def connect(self):
        client = Dropbox(self.access_key)
        client.users_get_current_account()
        return client

    def upload_files(self):
        pass

    def download_files(self):
        pass

    def move_or_rename_files(self):
        pass

    def remove_files(self):
        pass