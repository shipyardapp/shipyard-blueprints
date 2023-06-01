import argparse
import sys
from trello import TrelloClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--board-id", dest="board_id", required=True)
    parser.add_argument("--list-name", dest="list_name", required=True)
    parser.add_argument("--card-name", dest="card_name", required=True)
    parser.add_argument("--description", dest="description", required=False, default=None)
    parser.add_argument("--due-date", dest="due_date", required=False, default=None)
    return parser.parse_args()


def main():
    args = get_args()
    args_dict = vars(args)
    trello = TrelloClient(access_token=args.access_token, api_key=args.api_key)
    args_dict.pop('access_token')
    args_dict.pop('api_key')

    create_ticket_args = {key: value for key, value in args_dict.items() if value not in (None, '')}

    try:
        trello.create_ticket(**create_ticket_args)
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
