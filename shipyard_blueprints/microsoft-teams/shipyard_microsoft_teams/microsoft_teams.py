import requests
from shipyard_templates import Messaging, ShipyardLogger, ExitCodeException

logger = ShipyardLogger.get_logger()


class MicrosoftTeamsClient(Messaging):
    BASE_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, access_token=None, webhook_url=None):
        self.webhook_url = webhook_url
        self._access_token = access_token

    @property
    def access_token(self):
        if self._access_token:
            return self._access_token
        elif self.webhook_url:
            logger.debug("Webhook URL provided. No access token required")
        else:
            raise ExitCodeException(
                "Invalid credentials provided. Please provide all the required credentials to connect to the service",
                Messaging.EXIT_CODE_INVALID_CREDENTIALS,
            )

    @access_token.setter
    def access_token(self, access_token):
        self._access_token = access_token

    def _request(self, endpoint, method="GET", header_override=None, **kwargs):
        headers = header_override or {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.request(
            method, f"{self.BASE_URL}/{endpoint}", headers=headers, **kwargs
        )
        if response.ok:
            return response.json()
        else:
            raise ExitCodeException(
                f"An error occurred when attempting to make a request to Microsoft teams."
                f"{response.text}",
                Messaging.EXIT_CODE_UNKNOWN_ERROR,
            )

    def post_message(
        self, message_content, team_id=None, channel_id=None, message_title=None
    ):
        """
        Posts a message to a channel in a team
        :param message_content: The content of the message
        :param team_id: The ID of the team
        :param channel_id: The ID of the channel
        :param message_title: The title of the message


        """

        try:
            if self.webhook_url:
                logger.debug("Posting message via webhook")
                self._post_message_via_webhook(message_content, message_title)
                return
            if not self.access_token:
                raise ExitCodeException(
                    "Access token or webhook url is required to post a message to a channel when not using a webhook",
                    Messaging.EXIT_CODE_INVALID_INPUT,
                )
            if not team_id or not channel_id:
                raise ExitCodeException(
                    "Team ID and Channel ID are required to post a message to a channel when not using a webhook",
                    Messaging.EXIT_CODE_INVALID_INPUT,
                )

            message_details = {
                "body": {"content": message_content, "contentType": "text"}
            }
            if message_title:
                message_details["subject"] = message_title

            response = self._request(
                f"teams/{team_id}/channels/{channel_id}/messages",
                method="POST",
                json=message_details,
            )
            logger.debug(f"Response: {response}")
            logger.info("Message posted successfully")

        except Exception:
            raise

    def _post_message_via_webhook(self, message_content, message_title):
        response = requests.post(
            self.webhook_url,
            json={"title": message_title, "text": message_content},
        )
        if response.ok:
            logger.info("Message sent successfully")
        else:
            raise ExitCodeException(
                "An error occurred when attempting to post message to Microsoft teams."
                f"{response.text}",
                Messaging.EXIT_CODE_UNKNOWN_ERROR,
            )

    def connect(self):
        try:
            response = requests.post(self.webhook_url, json={})
        except Exception as e:
            logger.authtest(f"Could not connect to Microsoft Teams due to {e}")
            return 1
        else:
            if response.text == "Invalid webhook URL":
                logger.auth_test("Invalid webhook URL")
                return 1
            elif response.text == "Summary or Text is required.":
                return 0
            else:
                logger.authtest(f"Unexpected error message: {response.text}")
                return 1
