import os
import sys
from shipyard_postgresql import PostgresClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        postgres = PostgresClient(
            user=os.getenv("POSTGRES_USERNAME"),
            pwd=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DATABASE"),
        )
        postgres.connect()
        sys.exit(0)
    except Exception as e:
        logger.authtest(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
