import os
import unittest
from shipyard_slack import SlackClient


class SlackClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.access_token = os.getenv("SLACK_TOKEN")

    def test_connect_with_valid_credentials(self):
        assert SlackClient(self.access_token).connect() == 0

    def test_connect_with_invalid_access_token(self):
        assert SlackClient("invalid_access_token").connect() == 1
        assert SlackClient("").connect() == 1
