import os
import sys
import duckdb
from shipyard_motherduck import MotherDuckClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()
logger.setLevel("AUTHTEST")


def main():
    try:
        client = MotherDuckClient(os.getenv("MOTHERDUCK_TOKEN"))
        client.connect()
        logger.authtest("Successfully connected to MotherDuck")
    except Exception as e:
        logger.authtest(
            f"Error connecting to MotherDuck with the provided access token. Message from server: {e}"
        )
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
