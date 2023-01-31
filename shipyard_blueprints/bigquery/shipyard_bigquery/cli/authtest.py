import argparse
from shipyard_bigquery import BigQueryClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account",
                        dest='service_account', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    creds = args.service_account
    google_client = BigQueryClient(creds)
    try:
        con = google_client.connect()
        google_client.logger.info("Successfully established a connection")
        return 0
    except Exception as e:
        google_client.logger.error("Could not establish a connection")
        return 1


if __name__ == "__main__":
    main()
