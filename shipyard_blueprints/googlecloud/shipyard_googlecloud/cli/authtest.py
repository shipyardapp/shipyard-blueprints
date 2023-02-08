import argparse
from shipyard_blueprints import GoogleCloudClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account",
                        dest='service_account', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    service_account = args.service_account
    client = GoogleCloudClient(service_account=service_account)
    try:
        client.connect()
        return 0
    except Exception as e:
        client.logger.error(
            "Could not connect to Google Cloud with the service account provided")
        return 1


if __name__ == "__main__":
    main()
