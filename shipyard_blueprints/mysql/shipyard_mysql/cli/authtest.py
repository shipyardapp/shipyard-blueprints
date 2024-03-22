import os
import sys
from shipyard_mysql import MySqlClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        mysql = MySqlClient(
            username=os.getenv("MYSQL_USERNAME"),
            pwd=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST"),
            database=os.getenv("MYSQL_DATABASE"),
        )
        mysql.connect()
        logger.authtest(f"Connected to MySQL")
        sys.exit(0)
    except Exception as e:
        logger.authtest(
            f"Error connecting to MySQL. Message from the server reads: {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
