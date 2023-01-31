from templates.etl import Etl


class FivetranClient(Etl):
    def __init__(self, access_token: str, api_secret: str) -> None:
        self.api_secret = api_secret
        super().__init__(access_token)

    def execute_request():
        pass

    def trigger_sync(self):
        pass

    def determine_sync_status(self):
        pass

    def update_connector(self):
        pass
