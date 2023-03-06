from shipyard_blueprints import GoogleDriveClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--service-account',
                        dest='service_account', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    service_account = args.service_account
    drive = GoogleDriveClient(service_account)
    try:
        drive.connect()
        drive.logger.info("Successfully connected to Google Drive")
        return 0
    except Exception as e:
        drive.logger.error(
            "Could not connect to Google Drive with the service account provided")
        return 1


if __name__ == '__main__':
    main()
