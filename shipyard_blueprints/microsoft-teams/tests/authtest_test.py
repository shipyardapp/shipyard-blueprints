import os
import unittest
from unittest.mock import patch

from shipyard_microsoft_teams.cli.authtest import main


class MicrosoftTeamAuthtestTestCase(unittest.TestCase):
    @patch("sys.exit")
    def test_main_with_valid_webhook(self, mock_exit):
        os.environ["MICROSOFT_TEAMS_WEBHOOK_URL"] = os.getenv(
            "TEST_MICROSOFT_TEAMS_WEBHOOK_URL"
        )
        main()
        mock_exit.assert_called_with(0)

    @patch("sys.exit")
    def test_main_with_invalid_webhook(self, mock_exit):
        os.environ["MICROSOFT_TEAMS_WEBHOOK_URL"] = (
            "https://shipyardapp.webhook.office.com/webhookb2/invalid_webhook_url"
        )
        main()
        mock_exit.assert_called_with(1)

    @patch("sys.exit")
    def test_main_with_empty_webhook(self, mock_exit):
        os.environ["MICROSOFT_TEAMS_WEBHOOK_URL"] = ""
        main()
        mock_exit.assert_called_with(1)


if __name__ == "__main__":
    unittest.main()
