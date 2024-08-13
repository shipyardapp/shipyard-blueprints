import argparse
import os
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException, Messaging

from shipyard_microsoft_teams import MicrosoftTeamsClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser(
        description="Send a message to Microsoft Teams using an incoming webhook"
    )
    parser.add_argument(
        "--webhook-url", required=False, help="URL of the incoming webhook"
    )
    parser.add_argument(
        "--message-content", required=True, help="Content of the message to be sent"
    )
    parser.add_argument(
        "--message-title", required=False, help="Title of the message to be sent"
    )

    parser.add_argument("--team-id", required=False, help="Team ID to post message to")
    parser.add_argument(
        "--channel-id", required=False, help="Channel ID to post message to"
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()

        client = MicrosoftTeamsClient(webhook_url=args.webhook_url, access_token=os.getenv("OAUTH_ACCESS_TOKEN"))
        client.post_message(message_content=args.message_content, message_title=args.message_title,
                            team_id=args.team_id, channel_id=args.channel_id)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An Unknown error occurred: {e}")
        sys.exit(Messaging.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
