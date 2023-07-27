import os
import sys
from shipyard_blueprints import RudderStackClient


def main():
    client = RudderStackClient(access_token=os.environ.get("RUDDERSTACK_ACCESS_TOKEN"))
    conn = client.connect()
    sys.exit(0 if conn == 200 else 1)


if __name__ == "__main__":
    main()
