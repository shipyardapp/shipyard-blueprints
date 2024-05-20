import os
import unittest
from shipyard_jira import JiraClient
from dotenv import load_dotenv, find_dotenv
import pytest

CREDENTIALS = [
    "JIRA_ACCESS_TOKEN",
    "JIRA_DOMAIN",
    "JIRA_EMAIL",
]

@pytest.fixture(scope="module", autouse=True)
def get_env():
    load_dotenv(find_dotenv())
    if any(
            key not in os.environ
            for key in CREDENTIALS
    ):
        pytest.skip("Missing one or more required environment variables")

class JiraClientConnectTestCase(unittest.TestCase):
    def setUp(self):
        self.access_token = os.getenv("JIRA_ACCESS_TOKEN")
        self.domain = os.getenv("JIRA_DOMAIN")
        self.email_address = os.getenv("JIRA_EMAIL")

    def test_connect_with_valid_credentials(self):
        assert (
            JiraClient(self.access_token, self.domain, self.email_address).connect()
            == 0
        )

    def test_connect_with_invalid_domain(self):
        assert (
            JiraClient(
                self.access_token, "invalid_domain", self.email_address
            ).connect()
            == 1
        )
        assert JiraClient(self.access_token, "", self.email_address).connect() == 1

    def test_connect_with_invalid_email(self):
        assert (
            JiraClient(self.access_token, self.domain, "invalid_email").connect() == 1
        )
        assert JiraClient(self.access_token, self.domain, "").connect() == 1
        assert JiraClient(self.access_token, self.domain, "@gmail").connect() == 1

    def test_connect_with_invalid_access_token(self):
        assert (
            JiraClient("invalid_token", self.domain, self.email_address).connect() == 1
        )
        assert JiraClient("", self.domain, self.email_address).connect() == 1

    def test_connect_with_valid_access_token_and_invalid_email_and_domain(self):
        assert (
            JiraClient(self.access_token, "invalid_domain", "invalid_email").connect()
            == 1
        )
        assert JiraClient(self.access_token, "", "").connect() == 1
        assert JiraClient(self.access_token, "invalid_domain", "").connect() == 1
        assert JiraClient(self.access_token, "", "invalid_email").connect() == 1
