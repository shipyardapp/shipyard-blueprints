import requests
import json
from requests.auth import HTTPBasicAuth
from shipyard_templates import DataVisualization


class ModeClient(DataVisualization):
    def __init__(self, api_token: str, api_secret: str, account: str) -> None:
        self.api_token = api_token
        self.api_secret = api_secret
        self.account = account
        self.mode_api_base = f"https://app.mode.com/api/{self.account}"
        super().__init__(api_token=api_token, api_secret=api_secret, account=account)

    def connect(self):
        response = requests.get(url=self.mode_api_base, auth=HTTPBasicAuth(
            self.api_token, self.api_secret))
        return response
