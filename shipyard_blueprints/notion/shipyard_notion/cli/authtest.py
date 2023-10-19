import os
import sys
from shipyard_notion import NotionClient


def main():
    client = NotionClient(token=os.getenv("NOTION_ACCESS_TOKEN"))
    try:
        c = client.connect()
    except Exception as e:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
