import os
import sys
from shipyard_bigquery import BigQueryClient
from shipyard_templates import ShipyardLogger


logger = ShipyardLogger.get_logger()


def main():
    google_client = BigQueryClient(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    try:
        google_client.connect()
        logger.info("Successfully established a connection")
        sys.exit(0)
    except Exception as e:
        logger.error("Could not establish a connection")
        logger.debug(f"Response from Google BigQuery API: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
