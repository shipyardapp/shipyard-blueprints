import os
import sys
from shipyard_notion import NotionClient


def main():
    try:
        client = NotionClient(token=os.getenv("NOTION_ACCESS_TOKEN"))
        c = client.connect()
    except Exception as e:
        print(f"Could not connect to Notion with the provided access token due to {e}")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
