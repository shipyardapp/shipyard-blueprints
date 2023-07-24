import argparse
import os
from shipyard_bigquery import BigQueryClient


def main():
    google_client = BigQueryClient(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))
    try:
        con = google_client.connect()
        google_client.logger.info("Successfully established a connection")
        return 0
    except Exception as e:
        google_client.logger.error("Could not establish a connection")
        return 1


if __name__ == "__main__":
    main()
