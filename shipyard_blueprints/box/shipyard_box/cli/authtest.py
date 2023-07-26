import os
import sys
from shipyard_blueprints import BoxClient

def main():
    box = BoxClient(os.getenv('BOX_APPLICATION_CREDENTIALS'))
    try:
        box.connect()
        box.logger.info("Successfully connected to Box")
        box.logger.info(f"Creds are {box.service_account}")
        return 0
    except Exception as e:
        box.logger.error(
            "Could not connect to Box with the provided service account")
        return 1


if __name__ == '__main__':
    main()
