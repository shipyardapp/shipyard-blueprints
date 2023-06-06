import argparse
import sys
from shipyard_trello import TrelloClient


def get_args():
    parser = argparse.ArgumentParser(description='Update a Trello card.')
    parser.add_argument('--access-token', required=True, help='Your Trello access token.')
    parser.add_argument('--api-key', required=True, help='Your Trello API key.')
    parser.add_argument('--card-id', required=True, help='The ID of the Trello card you wish to update.')
    parser.add_argument('--board-id', help='The ID of the Trello board you wish to move the card to.')
    parser.add_argument('--list-name', help='The name of the Trello list you wish to move the card to.')
    parser.add_argument('--card-name', help='The updated card name.')
    parser.add_argument('--description', help='The description of the Trello card.')
    parser.add_argument('--due_date', help='The due date of the Trello card.')
    return parser.parse_args()


def main():
    args = get_args()
    args_dict = vars(args)
    trello = TrelloClient(access_token=args.access_token,
                          api_key=args.api_key)
    args_dict.pop('access_token')
    args_dict.pop('api_key')

    # Filter out blank values from update_ticket_args to avoid sending them to the Shortcut API
    # and inadvertently overwriting valid ticket data.
    update_card_args = {key: value for key, value in args_dict.items() if value not in (None, '')}

    try:
        trello.update_ticket(**update_card_args)

    except Exception as error:
        trello.logger.error(error)
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
