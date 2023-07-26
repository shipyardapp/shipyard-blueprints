import os
import unittest
from shipyard_clickup import ClickupClient


class ClickupClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.access_token = os.getenv('CLICKUP_ACCESS_TOKEN')
        self.client = ClickupClient(self.access_token)

    def test_connect_with_valid_credentials(self):
        assert self.client.connect()==0
    def test_connect_with_invalid_access_token(self):
        self.client.access_token = 'invalid_token'
        assert self.client.connect() == 1
        self.client.access_token = ''
        assert self.client.connect() == 1
