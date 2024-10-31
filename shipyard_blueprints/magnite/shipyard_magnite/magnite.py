import json
import requests
from shipyard_templates import DigitalAdverstising


class MagniteClient(DigitalAdverstising):
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.base_url = "https://console.springserve.com/api/v0"

    def connect(self):
        """
        Queries the API for an access token with the given username and password
        """
        url = f"{self.base_url}/auth"

        payload = json.dumps({"email": self.username, "password": self.password})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers, data=payload)
        if response.ok:
            self.token = response.json().get("token")
            return 0
        else:
            return 1

    def update(self, **kwargs):
        pass

    def create(self, **kwargs):
        pass

    def delete(self, **kwargs):
        pass

    def read(self, **kwargs):
        pass
