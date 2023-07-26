import unittest
import os
from shipyard_thoughtspot import ThoughtSpotClient


class TestThoughtSpotClient(unittest.TestCase):
    def setUp(self):
        self.token = os.getenv("THOUGHTSPOT_TOKEN")

    def test_connection_with_valid_token(self):
        assert ThoughtSpotClient(token=self.token).connect() == 0

    def test_connection_with_invalid_token(self):
        assert ThoughtSpotClient(token="invalid_token").connect() == 1
        assert ThoughtSpotClient(token="").connect() == 1
