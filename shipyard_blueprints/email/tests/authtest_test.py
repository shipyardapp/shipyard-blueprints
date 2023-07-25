import os
import unittest
from shipyard_email import EmailClient


class ModeClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = os.getenv('SMTP_PORT')
        self.username = os.getenv('SMTP_USERNAME')
        self.password = os.getenv('SMTP_PASSWORD')

    def test_connect_with_valid_credentials(self):
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password).connect() == 0

    def test_connection_with_invalid_port(self):
        assert EmailClient(self.smtp_host, '123', self.username, self.password).connect() == 1
        # assert EmailClient(self.smtp_host, '', self.username, self.password).connect() == 1

    def test_connection_with_invalid_host(self):
        assert EmailClient('invalid', self.smtp_port, self.username, self.password).connect() == 1
        assert EmailClient('', self.smtp_port, self.username, self.password).connect() == 1
        assert EmailClient('smtp.gmail', self.smtp_port, self.username, self.password).connect() == 1

    def test_connection_with_invalid_username(self):
        assert EmailClient(self.smtp_host, self.smtp_port, 'invalid', self.password).connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, '', self.password).connect() == 1

    def test_connection_with_invalid_password(self):
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, 'invalid').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, '').connect() == 1

    def test_connection_with_no_credentials(self):
        assert EmailClient(self.smtp_host, self.smtp_port, '', '').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, '', self.password).connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, '').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, '', '').connect() == 1

    def test_connection_with_credentials_swapped(self):
        assert EmailClient(self.smtp_host, self.smtp_port, self.password, self.username).connect() == 1

    def test_connection_with_invalid_method(self):
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password, 'invalid').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password, '').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password, 'ssl').connect() == 1

    def test_connection_with_no_method(self):
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password, '').connect() == 1
        assert EmailClient(self.smtp_host, self.smtp_port, self.username, self.password, '').connect() == 1
