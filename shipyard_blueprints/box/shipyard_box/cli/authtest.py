from shipyard_blueprints import BoxClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account",
                        dest='service_account', required=True, default=None)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    service_account = args.service_account
    box = BoxClient(service_account)
    try:
        box.connect()
        box.logger.info("Successfully connected to Box")
        return 0
    except Exception as e:
        box.logger.error(
            "Could not connect to Box with the provided service account")
        return 1


if __name__ == '__main__':
    main()
