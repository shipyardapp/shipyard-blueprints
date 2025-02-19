# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-trello",
# ]
# ///
import os
import sys
from shipyard_trello import TrelloClient


def main():
    sys.exit(
        TrelloClient(
            access_token=os.getenv("TRELLO_ACCESS_TOKEN"),
            api_key=os.getenv("TRELLO_API_KEY"),
        ).connect()
    )


if __name__ == "__main__":
    main()
