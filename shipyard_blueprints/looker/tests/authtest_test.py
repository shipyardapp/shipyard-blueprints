import os
import unittest
from shipyard_looker import LookerClient


class LookerClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.client_id = os.getenv("LOOKER_CLIENT_ID")
        self.client_secret = os.getenv("LOOKER_CLIENT_SECRET")
        self.looker_url = os.getenv("LOOKER_URL")

    def test_connect_with_valid_credentials(self):
        assert (
            LookerClient(
                base_url=self.looker_url,
                client_id=self.client_id,
                client_secret=self.client_secret,
            ).connect()
            == 0
        )

    def test_connect_with_invalid_client_id(self):
        assert (
            LookerClient(
                client_id="invalid",
                client_secret=self.client_secret,
                base_url=self.looker_url,
            ).connect()
            == 1
        )
        assert (
            LookerClient(
                client_id="", client_secret=self.client_secret, base_url=self.looker_url
            ).connect()
            == 1
        )

    def test_connect_with_invalid_client_secret(self):
        assert (
            LookerClient(
                client_id=self.client_id,
                client_secret="invalid",
                base_url=self.looker_url,
            ).connect()
            == 1
        )
        assert (
            LookerClient(
                client_id=self.client_id, client_secret="", base_url=self.looker_url
            ).connect()
            == 1
        )

    def test_connect_with_invalid_looker_url(self):
        assert (
            LookerClient(
                client_id=self.client_id,
                client_secret=self.client_secret,
                base_url="invalid",
            ).connect()
            == 1
        )
        assert (
            LookerClient(
                client_id=self.client_id, client_secret=self.client_secret, base_url=""
            ).connect()
            == 1
        )
        assert (
            LookerClient(
                client_id=self.client_id,
                client_secret=self.client_secret,
                base_url="https://invalid",
            ).connect()
            == 1
        )
