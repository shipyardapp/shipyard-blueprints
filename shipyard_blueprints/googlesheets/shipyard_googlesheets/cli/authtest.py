import os
from shipyard_blueprints import GoogleSheetsClient


def get_args():
    args = {}
    args['service_account'] = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    return args


def main():
    args = get_args()
    service_account = args['service_account']
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
