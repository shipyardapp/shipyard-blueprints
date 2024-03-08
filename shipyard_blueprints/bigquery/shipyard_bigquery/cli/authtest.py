import os
import sys
from shipyard_bigquery import BigQueryClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        google_client = BigQueryClient(
            os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        ).connect()
        logger.info(
            f"Successfully established a connection to BigQuery with service account associated with {google_client.email}"
        )
        sys.exit(0)
    except Exception as e:
        logger.error("Could not establish a connection")
        logger.debug(f"Response from Google BigQuery API: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
