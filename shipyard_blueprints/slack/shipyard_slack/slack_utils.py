from shipyard_templates import ShipyardLogger
from shipyard_bp_utils.args import create_shipyard_link

logger = ShipyardLogger.get_logger()


def create_user_id_list(slack_client, users_to_notify, user_lookup_method):
    """
    Create a list of all users to be notified.
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


def create_name_tags(user_id_list):
    """
    Create a string that consists of all the user_id tags
    that will be added at the beginning of a message.
    """
    return "".join(
        (
            f"<@{user_id}> "
            if user_id not in ["channel", "here", "everyone"]
            else f"<!{user_id}> "
        )
        for user_id in user_id_list
    )


def format_user_list(slack_client, users_to_notify, user_lookuo_method):
    return (
        create_user_id_list(slack_client, users_to_notify, user_lookuo_method)
        if (users_to_notify := users_to_notify)
        else []
    )


def send_slack_message_with_file(
    slack_client, message, file, channel, include_in_thread=False
):
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
        message = message + "\n\n _(File could not be uploaded. Check log for details)_"
        slack_client.update_message(message, channel_id, timestamp)
        raise e
    else:
        download_link = file_response["file"]["url_private_download"]
        response = slack_client.update_message(
            message, channel_id, timestamp, download_link
        )
        return response


def _create_blocks(message, download_link=""):
    """
    Create blocks for the main message, a divider, and context that links to Shipyard.
    If a download link is provided, creates a button block to immediately start that download.
    For more information: https://api.slack.com/block-kit/building
    """

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

    if download_link == "":
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
