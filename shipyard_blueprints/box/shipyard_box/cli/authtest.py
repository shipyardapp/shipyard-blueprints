import os
from shipyard_blueprints import BoxClient


def get_args():
    args = {}
    args['application_credentials'] = os.getenv['BOX_APPLICATION_CREDENTIALS']
    return args


def main():
    args = get_args()
    service_account = args['application_credentials']
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
