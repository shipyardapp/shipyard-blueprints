import re

from typing import List, Any, Optional
from shipyard_templates import ShipyardLogger
from slack_sdk.web import SlackResponse
from shipyard_bp_utils.args import create_shipyard_link
from shipyard_templates import ExitCodeException, Messaging

logger = ShipyardLogger.get_logger()

# FILE_PATTERN = r"\{\{text:([^\{\}]+)\}\}"
FILE_PATTERN = re.compile(r"\{\{[^\{\}]+\}\}")


def extract_file_contents(message: str) -> str:
    """
    Extracts the contents of a file from a message, if a file pattern is present.

    Args:
        message (str): The message content.

    Returns:
        str: The contents of the file, if a file pattern is present.
    """
    if not re.search(FILE_PATTERN, message):
        return message

    file = _extract_filename(message)
    return _read_file(file, message)


def _extract_filename(message: str) -> str:
    """
    Extracts the filename from a file pattern in a message.
    Args:
        message (str): The message content.

    Returns:
        str: The filename extracted from the file pattern.
    Raises:
        ExitCodeException: If the file pattern does not have the correct prefix.
    """
    match = FILE_PATTERN.search(message)
    matched = match.group()[
        2:-2
    ].strip()  # Extract the matched text without the curly braces

    if not matched.startswith("text:"):
        raise ExitCodeException(
            "When using the file pattern, the parameter needs to be prefixed with 'text:'",
            Messaging.EXIT_CODE_INVALID_INPUT,
        )
    return matched[5:]  # Remove the 'text:' prefix


def _read_file(file_path: str, message: str) -> str:
    """
    Reads the contents of a file and appends it to a message.

    Args:
        file_path (str): The path to the file.
        message (str): The message content.
    Returns:
        str: The message content with the file contents appended.
    Raises:
        ExitCodeException: If the file cannot be read.
    """
    try:
        with open(file_path.strip(), "r") as f:
            content = f.read()
    except FileNotFoundError as e:
        raise ExitCodeException(
            f"Could not load the contents of file {file_path}. Make sure the file exists",
            Messaging.EXIT_CODE_FILE_NOT_FOUND,
        ) from e
    except Exception as e:
        raise ExitCodeException(
            f"Error opening file {file_path}. Make sure the file extension is provided",
            Messaging.EXIT_CODE_INVALID_INPUT,
        ) from e

    return FILE_PATTERN.sub("", message, count=1) + "\n\n" + content


def create_user_id_list(
    slack_client, users_to_notify: str, user_lookup_method: str
) -> List[str]:
    """
    Creates a list of user IDs based on a string of usernames to notify.

    Args:
        slack_client (SlackClient): The Slack client instance used for user lookup.
        users_to_notify (str): A comma-separated string of users to be notified.
        user_lookup_method (str): The method name to be used for user lookup via the Slack client.

    Returns:
        List[str]: A list of user IDs to be notified.
    """
    users_to_notify = [x.strip() for x in users_to_notify.split(",")]
    user_id_list = []
    for user in users_to_notify:
        if user in ["@here", "@channel", "@everyone"]:
            user_id_list.append(user.replace("@", ""))
        else:
            logger.info(f"Looking up {user}")
            user_id = slack_client.user_lookup(user, user_lookup_method).get("id")
            user_id_list.append(user_id)
    return user_id_list


def create_name_tags(user_id_list: List[str]) -> str:
    """
    Creates a formatted string of user mention tags for Slack messages.

    Args:
        user_id_list (List[str]): A list of user IDs.

    Returns:
        str: A string with formatted Slack mention tags.
    """
    return "".join(
        (
            f"<@{user_id}> "
            if user_id not in ["channel", "here", "everyone"]
            else f"<!{user_id}> "
        )
        for user_id in user_id_list
    )


def format_user_list(
    slack_client, users_to_notify: Optional[str], user_lookup_method: str
) -> List[str]:
    """
    Formats a list of user IDs from a given string of usernames, if provided.

    Args:
        slack_client (SlackClient): The Slack client instance used for user lookup.
        users_to_notify (Optional[str]): A comma-separated string of users to be notified, or None.
        user_lookup_method (str): The method name to be used for user lookup via the Slack client.

    Returns:
        List[str]: A list of formatted user IDs.
    """
    return (
        create_user_id_list(slack_client, users_to_notify, user_lookup_method)
        if users_to_notify
        else []
    )


def send_slack_message_with_file(
    slack_client,
    message: str,
    file: Any,
    channel: str,
    include_in_thread: bool = False,
) -> SlackResponse:
    """
    Sends a Slack message with an attached file to a specified channel, optionally in a thread.

    Args:
        slack_client (SlackClient): The Slack client instance used for sending messages and files.
        message (str): The message content.
        file (Any): The file to be uploaded. The actual type depends on the Slack client's implementation.
        channel (str): The channel ID where the message and file will be sent.
        include_in_thread (bool, optional): Whether to include the message and file in a thread. Defaults to False.

    Returns:
        SlackResponse: The response from the Slack API after sending the message and file.
    """
    logger.debug(f"Attempting to send message with file to {channel}...")
    if not include_in_thread:
        logger.debug("Sending message without file in thread...")
        slack_client.send_message(message, channel)
        return slack_client.upload_file(file, channel)

    logger.debug("Sending message with file in thread...")
    message_with_file_status = message + "\n\n _(File is currently uploading...)_"
    response = slack_client.send_message(message_with_file_status, channel)
    channel_id = response["channel"]
    timestamp = response["ts"]

    try:
        logger.debug("Attempting to upload file in thread...")
        file_response = slack_client.upload_file(file, channel_id, timestamp)
    except Exception as e:
        message += "\n\n _(File could not be uploaded. Check log for details)_"
        slack_client.update_message(message, channel_id, timestamp)
        raise e
    else:
        download_link = file_response["file"]["url_private_download"]
        response = slack_client.update_message(
            message, channel_id, timestamp, download_link
        )
        return response


def _create_blocks(message: str, download_link: str = "") -> List[dict]:
    """
    Creates a list of block elements for a Slack message, optionally including a download button.

    Args:
        message (str): The main message content.
        download_link (str, optional): A URL for downloading an attached file. If provided, a download button is included.

    Returns:
        List[dict]: A list of Slack block elements for constructing a message.
    """

    message = extract_file_contents(message)

    message_section = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": message, "verbatim": True},
    }
    divider_section = {"type": "divider"}
    context_section = {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"Sent by Shipyard | <{create_shipyard_link()}|Click Here to Edit>",
            }
        ],
    }

    if not download_link:
        return [message_section, divider_section, context_section]

    download_section = {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Download File"},
                "value": "file_download",
                "url": download_link,
                "style": "primary",
            }
        ],
    }
    return [message_section, download_section, divider_section, context_section]
