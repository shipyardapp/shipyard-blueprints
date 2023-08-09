import os
import sys
from shipyard_blueprints import LookerClient


def main():
    sys.exit(
        LookerClient(
            client_id=os.getenv("LOOKER_CLIENT_ID"),
            client_secret=os.getenv("LOOKER_CLIENT_SECRET"),
            base_url=os.getenv("LOOKER_URL"),
        ).connect()
    )


if __name__ == "__main__":
    main()
