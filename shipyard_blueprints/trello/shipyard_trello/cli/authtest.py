import os
from shipyard_trello import TrelloClient


def main():
    return TrelloClient(access_token=os.getenv('TRELLO_ACCESS_TOKEN'), api_key=os.getenv('TRELLO_API_KEY')).connect()


if __name__ == '__main__':
    main()
