import os
import sys
import requests


def main():
    token = os.getenv('NOTION_ACCESS_TOKEN')
    headers = {'Authorization': f'Bearer {token}',
               'accept': 'application/json',
               'Notion-Version': '2022-06-28'}
    url = 'https://api.notion.com/v1/users/me'
    resp = requests.get(url, headers = headers)
    if not resp.ok:
        print(f"Could not connect to Notion with the provided access token: {resp.text}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
