import argparse
import sys

from shipyard_templates import ShipyardLogger, ProjectManagement

from shipyard_trello import TrelloClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser(description="Add comment to a Trello card")
    parser.add_argument(
        "--access-token", required=True, help="Access token for Trello API"
    )
    parser.add_argument("--api-key", required=True, help="API Key for Trello API")
    parser.add_argument("--card-id", required=True, help="ID of the Trello card")
    parser.add_argument("--comment", required=True, help="Comment to add to the card")

    return parser.parse_args()


def main():
    args = get_args()
    trello = TrelloClient(access_token=args.access_token, api_key=args.api_key)

    try:
        trello.add_comment(card_id=args.card_id, comment=args.comment)
    except Exception as error:
        logger.error(error)
        if error in (
            "Request failed with status code 401: invalid key",
            "Request failed with status code 401: invalid token",
        ):
            sys.exit(ProjectManagement.EXIT_CODE_INVALID_CREDENTIALS)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
