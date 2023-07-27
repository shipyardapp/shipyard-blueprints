import requests
import os
import sys


def main():
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {"Authorization": f"Bearer {os.getenv('ASANA_ACCESS_TOKEN')}"}
    resp = requests.get(url=url, headers=headers)
    if resp.status_code == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
