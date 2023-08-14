import os
import unittest
from shipyard_shortcut import ShortcutClient


class ShortcutClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.access_token = os.getenv("SHORTCUT_ACCESS_TOKEN")

    def test_connect_with_valid_credentials(self):
        assert ShortcutClient(access_token=self.access_token).connect() == 0

    def test_connect_with_invalid_access_token(self):
        assert ShortcutClient(access_token="invalid_access_token").connect() == 1
        assert ShortcutClient(access_token="").connect() == 1
