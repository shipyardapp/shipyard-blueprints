from shipyard_rudderstack import RudderStackClient
import unittest
import os

token = os.environ['RUDDERSTACK_TOKEN']
source_id = os.environ['RUDDERSTACK_SOURCE']


class IncorrectCredentials(unittest.TestCase):
    def test_credentials(self):
        rd = RudderStackClient('bad_token')
        resp = rd.trigger_sync(source_id)
        self.assertEqual(resp, rd.EXIT_CODE_INVALID_CREDENTIALS)


class InvalidSource(unittest.TestCase):
    def test_source(self):
        rd = RudderStackClient(token)
        resp = rd.trigger_sync("bad_source")
        self.assertEqual(resp, rd.EXIT_CODE_SYNC_INVALID_SOURCE_ID)


class GoodResponse(unittest.TestCase):
    """
    Tests for a correct response. Proper response should be a json object
    """

    def test_response(self):
        rd = RudderStackClient(token)
        response = rd.get_source_data(source_id)
        self.assertIsInstance(response, dict)


class BadResponse(unittest.TestCase):
    """ 
    Tests for an incorrect response. An incorrect response will be an int
    """

    def test_bad_response(self):
        rd = RudderStackClient(token)
        response = rd.get_source_data("bad_source")
        self.assertIsInstance(response, int)


unittest.main()
