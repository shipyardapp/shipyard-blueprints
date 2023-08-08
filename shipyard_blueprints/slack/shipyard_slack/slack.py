from slack import WebClient
import os
from shipyard_templates import Messaging


class SlackClient(Messaging):
    TIMEOUT = 120

    def __init__(self, slack_token: str) -> None:
        self.slack_token = slack_token
        super().__init__(slack_token=slack_token)

    def _set_environment_variabls(token: str):
        if token:
            os.environ["SLACK_BOT_TOKEN"] = token

    def connect(self):
        try:
            WebClient(token=self.slack_token, timeout=self.TIMEOUT).auth_test()
            self.logger.info("Successfully connected to Slack")
        except Exception as e:
            self.logger.error(f"Could not connect to Slack due to {e}")
            return 1
        else:
            return 0
