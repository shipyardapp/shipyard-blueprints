import os
import sys
from shipyard_blueprints import CoalesceClient


def main():
    client = CoalesceClient(os.getenv('COALESCE_ACCESS_TOKEN'))
    response = client.connect()
    if response == 200:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
