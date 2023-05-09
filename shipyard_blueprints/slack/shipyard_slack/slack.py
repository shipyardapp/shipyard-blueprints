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
            os.environ['SLACK_BOT_TOKEN'] = token

    def connect(self):
        slack_connection = WebClient(
            token=self.slack_token, timeout=self.TIMEOUT)
        return slack_connection
