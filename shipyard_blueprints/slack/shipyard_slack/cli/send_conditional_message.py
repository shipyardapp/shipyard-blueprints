import os
import sys
import argparse

from shipyard_slack import SlackClient
from shipyard_bp_utils.artifacts import Artifact
from shipyard_bp_utils import files as file_utils
from shipyard_templates import ShipyardLogger, ExitCodeException, Messaging
from shipyard_slack.slack_utils import (
    format_user_list,
    create_name_tags,
    send_slack_message_with_file,
)

BYTE_MAX = 1000000000

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--destination-type",
        dest="destination_type",
        default="channel",
        required=True,
        choices={"channel", "dm"},
    )
    parser.add_argument("--channel-name", dest="channel_name", required=False)
    parser.add_argument(
        "--user-lookup-method",
        dest="user_lookup_method",
        default="email",
        choices={"display_name", "real_name", "email"},
        required=False,
    )
    parser.add_argument("--users-to-notify", dest="users_to_notify", required=False)
    parser.add_argument("--message", dest="message", required=True)
    parser.add_argument(
        "--file-upload",
        dest="file_upload",
        default="no",
        required=True,
        choices={"yes", "no"},
    )
    parser.add_argument(
        "--conditional-send",
        dest="conditional_send",
        default="always",
        required=False,
        choices={"file_exists", "file_dne", "always"},
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=False)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--slack-token", dest="slack_token", required=True)

    parser.add_argument(
        "--include-file-in-thread",
        dest="include_file_in_thread",
        default="yes",
        required=False,
    )

    return parser.parse_args()


def validate_args(args):
    if args.destination_type == "channel" and not args.channel_name:
        raise ExitCodeException(
            "--destination-type channel requires --channel-name",
            Messaging.EXIT_CODE_INVALID_INPUT,
        )
    elif args.destination_type == "dm" and not args.users_to_notify:
        raise ExitCodeException(
            "--destination-type dm requires --users-to-notify",
            Messaging.EXIT_CODE_INVALID_INPUT,
        )

    if args.users_to_notify and not args.user_lookup_method:
        raise ExitCodeException(
            "--users-to-notify requires a --user-lookup-method",
            Messaging.EXIT_CODE_INVALID_INPUT,
        )

    if args.file_upload == "yes" and (
        not args.source_file_name_match_type or not args.source_file_name
    ):
        raise ExitCodeException(
            "--file-upload yes requires --source-file-name and --source-file-name-match-type",
            Messaging.EXIT_CODE_INVALID_INPUT,
        )


def main():
    try:
        responses = []

        args = get_args()
        validate_args(args)

        slack_client = SlackClient(args.slack_token)

        artifact = Artifact("slack")

        message = args.message
        source_folder_name = file_utils.clean_folder_name(args.source_folder_name)
        conditional_send = args.conditional_send
        file_upload = args.file_upload == "yes"
        include_in_thread = args.include_file_in_thread == "yes"

        files = file_utils.find_matching_files(
            args.source_file_name, source_folder_name, args.source_file_name_match_type
        )
        if len(files) > 1:
            upload = file_utils.compress_files(files, "archive", ".zip")
        elif len(files) == 1 and file_utils.are_files_too_large(files, BYTE_MAX):
            upload = file_utils.compress_files(files, files[0], ".zip")
        elif len(files) == 1:
            upload = files[0]
        else:
            upload = None

        if conditional_send == "file_exists":
            if not upload:
                raise ExitCodeException(
                    "File(s) could not be found. Message not sent.",
                    slack_client.EXIT_CODE_FILE_NOT_FOUND,
                )
        elif conditional_send == "file_dne":
            if upload:
                raise ExitCodeException(
                    "File(s) were found, but message was conditional based on file not existing. Message not sent.",
                    slack_client.EXIT_CODE_CONDITIONAL_SEND_NOT_MET,
                )

        user_id_list = format_user_list(
            slack_client, args.users_to_notify, args.user_lookup_method
        )

        if args.destination_type == "dm" and file_upload:
            for user_id in user_id_list:
                logger.info(f"Sending message with file to {user_id}...")
                response = send_slack_message_with_file(
                    slack_client, message, files, user_id, include_in_thread
                )
                responses.append(response.data)
        elif args.destination_type == "dm" and not file_upload:
            for user_id in user_id_list:
                logger.info(f"Sending message to {user_id}...")
                response = slack_client.send_message(
                    message=message, channel_name=user_id
                )
                responses.append(response.data)
        elif file_upload:
            logger.info(f"Sending message with file to {args.channel_name}...")
            response = send_slack_message_with_file(
                slack_client,
                create_name_tags(user_id_list) + message,
                upload,
                args.channel_name,
                include_in_thread,
            )

            responses.append(response.data)

        else:
            logger.info(f"Sending message to {args.channel_name}...")
            response = slack_client.send_message(
                message=create_name_tags(user_id_list) + message,
                channel_name=args.channel_name,
            )
            responses.append(response.data)
        artifact.responses.write_json(
            os.getenv("SHIPYARD_BLUEPRINT_NAME", "send_conditional_message"), responses
        )
    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(e)
        sys.exit(Messaging.EXIT_CODE_UNKNOWN_ERROR)
    else:
        logger.info("Message(s) sent successfully")


if __name__ == "__main__":
    main()
