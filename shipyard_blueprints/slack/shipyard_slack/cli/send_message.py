import os
import sys
import json
import argparse

from shipyard_slack import SlackClient
from shipyard_slack.slack_utils import create_user_id_list, create_name_tags
from shipyard_templates import ShipyardLogger, ExitCodeException, Messaging
from shipyard_bp_utils.artifacts import Artifact

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

    parser.add_argument("--slack-token", dest="slack_token", required=True)

    return parser.parse_args()


def main():
    try:
        responses = []
        artifact = Artifact("slack")

        args = get_args()

        slack_client = SlackClient(args.slack_token)

        if args.destination_type == "channel" and not args.channel_name:
            logger.error("--destination-type channel requires --channel-name")
            raise ExitCodeException(
                "--destination-type channel requires --channel-name",
                slack_client.EXIT_CODE_INVALID_INPUT,
            )
        elif args.destination_type == "dm" and not args.users_to_notify:
            raise ExitCodeException(
                "--destination-type dm requires --users-to-notify",
                slack_client.EXIT_CODE_INVALID_INPUT,
            )

        if args.users_to_notify and not args.user_lookup_method:
            logger.error("--users-to-notify requires a --user-lookup-method")

        destination_type = args.destination_type
        channel_name = args.channel_name
        message = args.message

        user_id_list = create_user_id_list(
            slack_client, args.users_to_notify, args.user_lookup_method
        )

        if destination_type == "dm":
            for user_id in user_id_list:
                response = slack_client.send_message(
                    message=message, channel_name=user_id
                )
                responses.append(response.data)

        else:
            response = slack_client.send_message(
                message=create_name_tags(user_id_list) + message,
                channel_name=channel_name,
            )
            responses.append(response.data)

        artifact.responses.write_json(
            os.getenv("SHIPYARD_BLUEPRINT_NAME", "send_message"), responses
        )

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(Messaging.EXIT_CODE_UNKNOWN_ERROR)

    else:
        logger.info("Message(s) sent successfully.")


if __name__ == "__main__":
    main()
