from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from shipyard_templates import Messaging, ShipyardLogger, ExitCodeException
from shipyard_slack.slack_utils import _create_blocks

logger = ShipyardLogger().get_logger()


class SlackClient(Messaging):
    TIMEOUT = 120
    EXIT_CODE_USER_NOT_FOUND = 100
    EXIT_CODE_CONDITIONAL_SEND_NOT_MET = 101

    def __init__(self, slack_token: str) -> None:
        self.slack_token = slack_token
        self.web_client = WebClient(token=self.slack_token, timeout=self.TIMEOUT)

    def _handle_slack_error(self, slack_error: SlackApiError):
        error_type = slack_error.response.get("error")

        if error_type == "invalid_auth":
            logger.error("Invalid Slack token")
            raise ExitCodeException(
                "Invalid Slack token", self.EXIT_CODE_INVALID_CREDENTIALS
            )
        elif error_type == "ratelimited":
            logger.error("Rate limited by Slack")
            raise ExitCodeException("Rate limited by Slack", self.EXIT_CODE_RATE_LIMIT)
        else:
            logger.error(f"Response from Slack servers: {slack_error}")
            raise ExitCodeException(
                f" Response from Slack servers: {slack_error}",
                self.EXIT_CODE_UNKNOWN_ERROR,
            )

    def connect(self):
        try:
            self.web_client.auth_test()
            logger.authtest("Successfully connected to Slack")
        except SlackApiError as e:
            if e.response.get("error") == "invalid_auth":
                logger.authtest("Invalid Slack token")
            else:
                logger.authtest(
                    f"Failed to validate credentials. Response from Slack servers: {e}"
                )
            return 1
        else:
            return 0

    def send_message(self, message: str, channel_name: str, download_link: str = ""):
        """
        Send a Slack message to the channel of your choice.
        Channel should be provided without a #
        Blocks will contain the main message, with text serving as a backup.
        """
        logger.debug("Attempting to send message to Slack...")
        message_response = self.web_client.chat_postMessage(
            channel=channel_name,
            link_names=True,
            text=message,
            blocks=_create_blocks(message, download_link=download_link),
        )
        logger.debug(
            f'Your message of "{message}" was successfully sent to {channel_name}'
        )
        return message_response

    def user_lookup(self, lookup, lookup_method):
        """
        Look up a user's Slack ID, using a provided search value and a lookup method.
        """
        if lookup_method not in {"email", "real_name", "display_name"}:
            raise ExitCodeException(
                "Invalid lookup method. Please use 'email', 'real_name', or 'display_name'.",
                self.EXIT_CODE_INVALID_INPUT,
            )
        else:
            logger.debug(f"Attempting to look up user by {lookup_method}...")

        user_details = None
        if lookup_method == "email":
            user_details = self.search_user_by_email(lookup)
        elif lookup_method == "real_name":
            user_details = self.search_user_by_name(lookup)
        elif lookup_method == "display_name":
            user_details = self.search_user_by_display_name(lookup)
        return user_details

    def search_user_by_email(self, email_address):
        logger.debug(f"Attempting to look up user {email_address} by email...")
        try:
            response = self.web_client.users_lookupByEmail(email=email_address)
            logger.debug("User found by email")
            return response.data.get("user")
        except SlackApiError as e:
            self._handle_slack_error(e)

    def search_user_by_name(self, name):
        logger.debug(f"Attempting to look up user {name} by real name...")
        try:
            response = self.web_client.users_list()
            for user in response.data["members"]:
                if user.get("real_name") == name:
                    logger.debug("User found by real name")
                    return user
            raise ExitCodeException(
                f"User {name} not found", self.EXIT_CODE_USER_NOT_FOUND
            )
        except SlackApiError as e:
            self._handle_slack_error(e)

    def search_user_by_display_name(self, display_name):
        logger.debug("Attempting to look up user by display name...")
        try:
            response = self.web_client.users_list()

            for user in response.data["members"]:
                if user["profile"]["display_name"] == display_name:
                    logger.debug("User found by display name")
                    return user

            raise ExitCodeException(
                f"User {display_name} not found", self.EXIT_CODE_USER_NOT_FOUND
            )

        except SlackApiError as e:
            self._handle_slack_error(e)

    def update_message(self, message, channel_id, timestamp, download_link=""):
        logger.debug("Attempting to update message in Slack...")
        try:
            self.web_client.chat_update(
                channel=channel_id, link_names=True, text=message, ts=timestamp
            )
            response = self.web_client.chat_update(
                channel=channel_id,
                link_names=True,
                text=message,
                ts=timestamp,
                blocks=_create_blocks(message, download_link=download_link),
            )
            logger.debug("Your message was updated")
            return response
        except SlackApiError as e:
            self._handle_slack_error(e)

    def upload_file(self, filename, channels, thread_ts=None):
        logger.debug(f"Attempting to upload file {filename} to Slack...")
        try:
            if thread_ts:
                return self.web_client.files_upload(
                    file=filename, channels=channels, thread_ts=thread_ts
                )
            else:
                return self.web_client.files_upload(file=filename, channels=channels)
        except SlackApiError as e:
            self._handle_slack_error(e)
