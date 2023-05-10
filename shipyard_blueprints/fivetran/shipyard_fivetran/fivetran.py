import requests
from shipyard_templates import Etl

class FivetranClient(Etl):
    def __init__(self, access_token: str, api_secret: str = None) -> None:
        self.api_secret = api_secret
        self.api_key = access_token
        self.headers = requests.auth.HTTPBasicAuth(self.api_key, self.api_secret)
        super().__init__(self.api_key, api_secret = self.api_secret)

    def execute_request():
        pass

    def trigger_sync(self):
        pass

    def determine_sync_status(self):
        pass

    def update_connector(self):
        pass

    def connect(self) -> int:
        """ Connect to Fivetran API

        Returns:
            int: HTTP Status code
        """
        url = 'https://api.fivetran.com/v1/users'
        resp = requests.get(url, auth=self.headers)
        return resp.status_code


