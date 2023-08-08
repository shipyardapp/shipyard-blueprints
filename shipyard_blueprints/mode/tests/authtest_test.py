import os
import unittest
from shipyard_mode import ModeClient


class ModeClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.api_token = os.environ.get("MODE_TOKEN_ID")
        self.api_secret = os.getenv("MODE_TOKEN_PASSWORD")
        self.account = os.getenv("MODE_WORKSPACE_NAME")

    def test_connect_with_valid_credentials(self):
        assert (
            ModeClient(
                api_token=self.api_token,
                api_secret=self.api_secret,
                account=self.account,
            ).connect()
            == 0
        )

    def test_connect_with_invalid_token(self):
        assert (
            ModeClient(
                api_token="invalid_token",
                api_secret=self.api_secret,
                account=self.account,
            ).connect()
            == 1
        )
        assert (
            ModeClient(
                api_token="", api_secret=self.api_secret, account=self.account
            ).connect()
            == 1
        )

    def test_connect_with_invalid_password(self):
        assert (
            ModeClient(
                api_token=self.api_token, api_secret="invalid_key", account=self.account
            ).connect()
            == 1
        )
        assert (
            ModeClient(
                api_token=self.api_token, api_secret="", account=self.account
            ).connect()
            == 1
        )

    def test_connect_with_invalid_account(self):
        assert (
            ModeClient(
                api_token=self.api_token,
                api_secret=self.api_secret,
                account="invalid_account",
            ).connect()
            == 1
        )
        assert (
            ModeClient(
                api_token=self.api_token, api_secret=self.api_secret, account=""
            ).connect()
            == 1
        )

    def test_connection_with_all_invalid_credentials(self):
        assert (
            ModeClient(
                api_token="invalid", api_secret="invalid", account="invalid"
            ).connect()
            == 1
        )
        assert ModeClient(api_token="", api_secret="", account="").connect() == 1
        assert ModeClient(api_token="invalid", api_secret="", account="").connect() == 1
        assert (
            ModeClient(api_token="invalid", api_secret="invalid", account="").connect()
            == 1
        )
        assert (
            ModeClient(api_token="", api_secret="invalid", account="invalid").connect()
            == 1
        )
        assert ModeClient(api_token="", api_secret="invalid", account="").connect() == 1
        assert ModeClient(api_token="", api_secret="", account="invalid").connect() == 1
        assert (
            ModeClient(api_token="invalid", api_secret="", account="invalid").connect()
            == 1
        )

    def test_connect_with_swapped_credentials(self):
        assert (
            ModeClient(
                api_token=self.api_secret,
                api_secret=self.api_token,
                account=self.account,
            ).connect()
            == 1
        )
