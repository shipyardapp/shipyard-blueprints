import os
import sys
from shipyard_blueprints import AirbyteClient


def main():
    token = os.getenv('AIRBYTE_API_TOKEN')
    client = AirbyteClient(token)
    response = client.connect()
    if response == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
