import os
from shipyard_blueprints import GoogleDriveClient


def get_args():
    args = {}
    args['service_account'] = os.getenv('GOOGLE_APPLICATIONS_CREDENTIALS') 
    return args


def main():
    args = get_args()
    service_account = args['service_account']
    drive = GoogleDriveClient(service_account)
    try:
        conn = drive.connect()
        drive.logger.info("Successfully connected to Google Drive")
        return 0
    except Exception as e:
        drive.logger.error(
            "Could not connect to Google Drive with the service account provided")
        return 1


if __name__ == '__main__':
    main()
