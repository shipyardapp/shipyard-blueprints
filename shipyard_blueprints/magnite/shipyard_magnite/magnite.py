import json
import requests
from shipyard_templates import DigitalAdverstising, ShipyardLogger

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Any

logger = ShipyardLogger.get_logger()


class MagniteClient(DigitalAdverstising):
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        self.base_url = "https://console.springserve.com/api/v0"
        self.headers = {"Content-Type": "application/json"}

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
            self.headers["Authorization"] = self.token
            return 0
        else:
            return 1

    def update(self):
        pass

    def create(self, **kwargs):
        pass

    def delete(self, **kwargs):
        pass

    def read(self, endpoint: str, id: str) -> Dict[Any, Any]:
        """Returns the JSON from the springserve API for the given endpoint (assuming it is valid)

        Args:
            endpoint: The Springserve endpoint to query
            id: The ID of the item to fetch

        """
        endpoint_url = f"{self.base_url}/{endpoint}/{id}"
        response = requests.get(endpoint_url, headers=self.headers)
        response.raise_for_status()
        return response.json()
