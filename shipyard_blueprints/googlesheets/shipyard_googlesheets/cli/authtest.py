from shipyard_blueprints import GoogleSheetsClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account",
                        dest='service_account', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    service_account = args.service_account
    client = GoogleSheetsClient(service_account)
    try:
        service, drive = client.connect()
        client.logger.info("Successfully connected to google sheets")
        return 0
    except Exception as e:
        client.logger.error(
            "Could not connect to Google Sheets with the Service Account provided")
        return 1


if __name__ == "__main__":
    main()
