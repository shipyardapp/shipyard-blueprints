import os
import sys
from shipyard_motherduck import MotherDuckClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        print(f"Token is {os.getenv('MOTHERDUCK_TOKEN')}")
        client = MotherDuckClient(os.getenv("MOTHERDUCK_TOKEN"))
        client.connect()
    except Exception as e:
        logger.authtest.error(
            f"Error connecting to MotherDuck with the provided access token. Message from server: {e}"
        )
        sys.exit(1)
    else:
        sys.exit(0)
