import os
import sys
import duckdb
from shipyard_motherduck import MotherDuckClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        duckdb.sql(
            'SET home_directory= "/tmp"'
        )  # this is necessary for the lambda runtime
        client = MotherDuckClient(os.getenv("MOTHERDUCK_TOKEN"))
        client.connect()
        logger.info("Successfully connected to MotherDuck")
        logger.authtest("Successfully connected to MotherDuck")
    except Exception as e:
        logger.error("Error connecting to MotherDuck, message from server: {e}")
        logger.authtest(
            f"Error connecting to MotherDuck with the provided access token. Message from server: {e}"
        )
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
