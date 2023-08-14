import os
import unittest
from shipyard_hex import HexClient


class HexClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.api_token = os.getenv("HEX_API_TOKEN")
        self.project_id = os.getenv("HEX_PROJECT_ID")

    def test_connect_with_valid_credentials(self):
        assert (
            HexClient(api_token=self.api_token, project_id=self.project_id).connect()
            == 0
        )

    def test_connect_with_invalid_credentials(self):
        assert (
            HexClient(api_token="invalid_token", project_id=self.project_id).connect()
            == 1
        )
        assert (
            HexClient(
                api_token=self.api_token, project_id="invalid_project_id"
            ).connect()
            == 1
        )

    def test_connect_with_invalid_project_id(self):
        assert (
            HexClient(
                api_token=self.api_token, project_id="invalid_project_id"
            ).connect()
            == 1
        )
        assert HexClient(api_token=self.api_token, project_id="").connect() == 1
