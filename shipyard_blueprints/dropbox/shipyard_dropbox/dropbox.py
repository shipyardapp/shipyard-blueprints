from dropbox import Dropbox
from shipyard_templates import CloudStorage


class DropboxClient(CloudStorage):
    def __init__(self, access_key: str) -> None:
        self.access_key = access_key

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
