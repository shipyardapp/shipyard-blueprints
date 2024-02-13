from typing import Any, Optional, Dict
from slack_sdk import WebClient
from slack_sdk.web import SlackResponse
from slack_sdk.errors import SlackApiError
from shipyard_slack.slack_utils import _create_blocks
from shipyard_templates import Messaging, ShipyardLogger, ExitCodeException

logger = ShipyardLogger().get_logger()


class SlackClient(Messaging):
    """
    A client for interacting with Slack using the Slack SDK, providing functionalities such as sending messages,
    looking up users, updating messages, and uploading files to Slack channels.
    """

    TIMEOUT = 120
    EXIT_CODE_USER_NOT_FOUND = 100
    EXIT_CODE_CONDITIONAL_SEND_NOT_MET = 101
    EXIT_CODE_APP_NOT_IN_CHANNEL = 102

    def __init__(self, slack_token: str) -> None:
        """
        Initializes the SlackClient with a Slack token.

        Args:
            slack_token (str): The token used for authenticating with the Slack API.
        """
        self.slack_token = slack_token
        self.web_client = WebClient(token=self.slack_token, timeout=self.TIMEOUT)

    def _handle_slack_error(self, slack_error: SlackApiError) -> None:
        """
        Handles errors returned by the Slack API.

        Args:
            slack_error (SlackApiError): The error raised by a Slack API call.

        Raises:
            ExitCodeException: Custom exception with an error message and an exit code.
        """
        error_type = slack_error.response.get("error")

        if error_type == "invalid_auth":
            logger.error("Invalid Slack token")
            raise ExitCodeException(
                "Invalid Slack token", self.EXIT_CODE_INVALID_CREDENTIALS
            )
        elif error_type == "not_in_channel":
            logger.error("Bot not in channel")
            raise ExitCodeException(
                "The Slack App does not have access to the channel. Add the App to the channel and try again",
                self.EXIT_CODE_APP_NOT_IN_CHANNEL,
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

    def connect(self) -> int:
        """
        Tests the connection to Slack using the provided token.

        Returns:
            int: Returns 0 if connection is successful, 1 otherwise.
        """
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

    def send_message(
        self, message: str, channel_name: str, download_link: str = ""
    ) -> SlackResponse:
        """
        Sends a message to a specified Slack channel.

        Args:
        message (str): The message content to send.
        channel_name (str): The name of the channel to send the message to.
        download_link (str, optional): A URL for a downloadable resource to include in the message, if any.

        Returns:
        SlackResponse: The response from the Slack API after sending the message.
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

    def user_lookup(self, lookup: str, lookup_method: str) -> Optional[Dict]:
        """
        Looks up a user's Slack ID using a provided search value and method.

        Args:
        lookup (str): The search value to look up the user by.
        lookup_method (str): The method to use for the lookup ('email', 'real_name', or 'display_name').

        Returns:
        Optional[Dict]: The user's details if found, None otherwise.

        Raises:
        ExitCodeException: If the lookup method is invalid.
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

    def search_user_by_email(self, email_address: str) -> Optional[Dict]:
        """
        Looks up a user by their email address.

        Args:
            email_address (str): The email address of the user to look up.

        Returns:
            Optional[Dict]: The user's details if found, None otherwise.

        Raises:
            ExitCodeException: If the Slack API call fails.
        """
        logger.debug(f"Attempting to look up user {email_address} by email...")
        try:
            response = self.web_client.users_lookupByEmail(email=email_address)
            logger.debug("User found by email")
            return response.data.get("user")
        except SlackApiError as e:
            self._handle_slack_error(e)

    def search_user_by_name(self, name: str) -> Optional[Dict]:
        """
        Looks up a user by their real name.

        Args:
            name (str): The real name of the user to look up.

        Returns:
            Optional[Dict]: The user's details if found, None otherwise.

        Raises:
            ExitCodeException: If the Slack API call fails.
        """
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

    def search_user_by_display_name(self, display_name: str) -> Optional[Dict]:
        """
        Looks up a user by their display name.

        Args:
            display_name (str): The display name of the user to look up.

        Returns:
            Optional[Dict]: The user's details if found, None otherwise.

        Raises:
            ExitCodeException: If the Slack API call fails.
        """
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

    def update_message(
        self, message: str, channel_id: str, timestamp: str, download_link: str = ""
    ) -> SlackResponse:
        """
        Updates a previously sent Slack message.

        Args:
            message (str): The new message content to update.
            channel_id (str): The channel ID where the message was originally sent.
            timestamp (str): The timestamp of the original message to be updated.
            download_link (str, optional): A URL for a downloadable resource to include in the updated message, if any.

        Returns:
        SlackResponse: The response from the Slack API after updating the message.
        """
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

    def upload_file(
        self, filename: str, channels: str, thread_ts: Optional[str] = None
    ) -> SlackResponse:
        """
        Uploads a file to a Slack channel, optionally in a thread.

        Args:
            filename (str): The path to the file to upload.
            channels (str): The channel IDs where the file should be uploaded, separated by commas.
            thread_ts (Optional[str]): The thread timestamp to upload the file into, if any.

        Returns:
        SlackResponse: The response from the Slack API after uploading the file.
        """
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
