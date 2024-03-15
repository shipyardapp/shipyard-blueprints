from dropbox import Dropbox
from shipyard_templates import CloudStorage, ShipyardLogger

logger = ShipyardLogger.get_logger()


class DropboxClient(CloudStorage):
    def __init__(self, access_key: str) -> None:
        self.access_key = access_key

    def connect(self):
        try:
            Dropbox(self.access_key).users_get_current_account()
            logger.authtest("Successfully verified Dropbox access key.")
            return 0
        except Exception as e:
            logger.authtest(
                f"Failed to authenticate using key. Message from Dropbox Server{e}"
            )
            return 1

    def upload(self):
        pass

    def download(self):
        pass

    def move(self):
        pass

    def remove(self):
        pass
