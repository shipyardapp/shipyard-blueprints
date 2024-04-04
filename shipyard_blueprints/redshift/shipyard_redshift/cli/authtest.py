import os
import sys
from shipyard_redshift import RedshiftClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    host = os.getenv("REDSHIFT_HOST")
    user = os.getenv("REDSHIFT_USERNAME")
    pwd = os.getenv("REDSHIFT_PASSWORD")
    port = os.getenv("REDSHIFT_PORT")
    database = os.getenv("REDSHIFT_DATABASE")
    redshift = RedshiftClient(
        user=user, pwd=pwd, host=host, port=port, database=database
    )
    try:
        con = redshift.connect()
        logger.info("Connected to Redshift")
        sys.exit(0)
    except Exception as e:
        logger.authtest(
            f"Could not connect to Redshift with given credentials. Message from the server reads: {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
