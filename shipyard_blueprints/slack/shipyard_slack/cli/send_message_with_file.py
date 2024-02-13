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
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match", "glob_match"},
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


def main():
    try:
        responses = []

        args = get_args()
        validate_args(args)
        artifact = Artifact("slack")

        slack_client = SlackClient(args.slack_token)

        message = args.message
        source_folder_name = file_utils.clean_folder_name(args.source_folder_name)
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
        elif len(files) == 0:
            raise ExitCodeException(
                f"No files found with name {args.source_file_name}",
                slack_client.EXIT_CODE_FILE_NOT_FOUND,
            )

        user_id_list = format_user_list(
            slack_client, args.users_to_notify, args.user_lookup_method
        )

        if args.destination_type == "dm":
            for user_id in user_id_list:
                response = send_slack_message_with_file(
                    slack_client, message, upload, user_id, include_in_thread
                )
                responses.append(response.data)

        else:

            response = send_slack_message_with_file(
                slack_client,
                create_name_tags(user_id_list) + message,
                upload,
                args.channel_name,
                include_in_thread,
            )
            responses.append(response.data)

        artifact.responses.write_json(
            os.getenv("SHIPYARD_BLUEPRINT_NAME", "send_message_with_file"), responses
        )
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(e)
        sys.exit(Messaging.EXIT_CODE_UNKNOWN_ERROR)
    else:
        logger.info("Message(s) sent successfully")


if __name__ == "__main__":
    main()
