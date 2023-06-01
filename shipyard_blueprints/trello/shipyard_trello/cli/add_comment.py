import argparse
import sys
from trello import TrelloClient


def get_args():
    parser = argparse.ArgumentParser(description='Add comment to a Trello card')
    parser.add_argument('--access_token', required=True, help='Access token for Trello API')
    parser.add_argument('--api_key', required=True, help='API Key for Trello API')
    parser.add_argument('--card_id', required=True, help='ID of the Trello card')
    parser.add_argument('--comment', required=True, help='Comment to add to the card')

    return parser.parse_args()


def main():
    args = get_args()
    trello = TrelloClient(access_token=args.access_token,
                          api_key=args.api_key)

    try:
        trello.add_comment(card_id=args.card_id, comment=args.comment)
    except Exception as error:
        trello.logger.error(error)
        if error in ('Request failed with status code 401: invalid key',
                     'Request failed with status code 401: invalid token'):
            sys.exit(trello.EXIT_CODE_INVALID_CREDENTIALS)
        else:
            sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
