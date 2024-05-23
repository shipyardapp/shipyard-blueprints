import os
import unittest
from shipyard_trello import TrelloClient


class TrelloClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.access_token = os.getenv("TRELLO_ACCESS_TOKEN")
        self.api_key = os.getenv("TRELLO_API_KEY")
        self.client = TrelloClient(access_token=self.access_token, api_key=self.api_key)

    def test_connect_with_valid_credentials(self):
        assert self.client.connect() == 0

    def test_connect_with_invalid_access_token(self):
        self.client.auth = {"key": self.api_key, "token": "invalid_token"}
        assert self.client.connect() == 1
        self.client.auth = {"key": self.api_key, "token": ""}
        assert self.client.connect() == 1

    def test_connect_with_invalid_api_key(self):
        self.client.auth = {"key": "invalid_key", "token": self.access_token}
        assert self.client.connect() == 1
        self.client.auth = {"key": "", "token": self.access_token}
        assert self.client.connect() == 1

    def test_connect_with_swapped_credentials(self):
        self.client.auth = {"key": self.access_token, "token": self.api_key}

        assert self.client.connect() == 1
