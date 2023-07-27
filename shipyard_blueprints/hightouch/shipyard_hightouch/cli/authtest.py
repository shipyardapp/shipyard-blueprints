import os
import sys
from shipyard_blueprints import HightouchClient


def main():
    client = HightouchClient(access_token=os.environ.get('HIGHTOUCH_API_KEY'))
    conn = client.connect()
    if conn == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
