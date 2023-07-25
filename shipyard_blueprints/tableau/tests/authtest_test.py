import os
import unittest
from shipyard_tableau import TableauClient


class TableauClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.username = os.getenv('TABLEAU_USERNAME')
        self.password = os.getenv('TABLEAU_PASSWORD')
        self.server_url = os.getenv('TABLEAU_SERVER_URL')
        self.site_id = os.getenv('TABLEAU_SITE_ID')
        self.client = TableauClient(self.username, self.password, self.server_url, self.site_id)

    def test_connect_with_valid_credentials(self):
        assert TableauClient(self.username, self.password, self.server_url, self.site_id).connect('access_token') == 0

    def test_connect_with_invalid_username(self):
        assert TableauClient('invalid_username', self.password, self.server_url, self.site_id).connect(
            'access_token') == 1
        assert TableauClient('', self.password, self.server_url, self.site_id).connect('access_token') == 1

    def test_connect_with_invalid_password(self):
        assert TableauClient(self.username, 'invalid_password', self.server_url, self.site_id).connect(
            'access_token') == 1
        assert TableauClient(self.username, '', self.server_url, self.site_id).connect('access_token') == 1

    def test_connect_with_invalid_server_url(self):
        assert TableauClient(self.username, self.password, 'invalid_server_url', self.site_id).connect(
            'access_token') == 1
        assert TableauClient(self.username, self.password, '', self.site_id).connect('access_token') == 1
