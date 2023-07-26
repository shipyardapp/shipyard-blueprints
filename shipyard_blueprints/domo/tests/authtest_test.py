import os
import unittest
from shipyard_domo import DomoClient


class DomoClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.client_id = os.getenv('DOMO_CLIENT_ID')
        self.secret_key = os.getenv('DOMO_SECRET_KEY')
        self.access_token = os.getenv('DOMO_ACCESS_TOKEN')
        self.domo_instance = os.getenv('DOMO_INSTANCE_ID')

    def test_connect_with_valid_credentials(self):
        # Test Client ID and Secret Key
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key).connect() == 0
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, domo_instance='').connect() == 0
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token='',
                          domo_instance='').connect() == 0
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token='').connect() == 0

        # Test Access Token and Domo Instance ID
        assert DomoClient(access_token=self.access_token, domo_instance=self.domo_instance).connect() == 0
        assert DomoClient(access_token=self.access_token, domo_instance=self.domo_instance, client_id='',
                          secret_key='').connect() == 0
        assert DomoClient(access_token=self.access_token, domo_instance=self.domo_instance, client_id='').connect() == 0
        assert DomoClient(access_token=self.access_token, domo_instance=self.domo_instance,
                          secret_key='').connect() == 0

    def test_connect_with_invalid_client_id(self):
        assert DomoClient(client_id='invalid', secret_key=self.secret_key).connect() == 1
        assert DomoClient(client_id='', secret_key=self.secret_key).connect() == 1

    def test_connect_with_invalid_secret_key(self):
        assert DomoClient(client_id=self.client_id, secret_key='invalid').connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key='').connect() == 1

    def test_connect_with_invalid_client_id_and_secret_key(self):
        assert DomoClient(client_id='invalid', secret_key='invalid').connect() == 1
        assert DomoClient(client_id='', secret_key='').connect() == 1

    def test_connect_with_credentials_swapped(self):
        assert DomoClient(client_id=self.secret_key, secret_key=self.client_id).connect() == 1

    def test_connect_with_invalid_access_token(self):
        assert DomoClient(access_token='invalid', domo_instance=self.domo_instance).connect() == 1
        assert DomoClient(access_token='', domo_instance=self.domo_instance).connect() == 1

    def test_connect_with_invalid_domo_instance_id(self):
        assert DomoClient(access_token=self.access_token, domo_instance='invalid').connect() == 1
        assert DomoClient(access_token=self.access_token, domo_instance='').connect() == 1

    def test_connect_with_mix_valid_and_invalid_credentials(self):
        assert DomoClient(client_id=self.client_id, secret_key='invalid', access_token=self.access_token,
                          domo_instance=self.domo_instance).connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key='', access_token=self.access_token,
                          domo_instance=self.domo_instance).connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token='invalid',
                          domo_instance=self.domo_instance).connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token='',
                          domo_instance=self.domo_instance).connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token=self.access_token,
                          domo_instance='invalid').connect() == 1
        assert DomoClient(client_id=self.client_id, secret_key=self.secret_key, access_token=self.access_token,
                          domo_instance='').connect() == 1
