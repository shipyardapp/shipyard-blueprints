import sys
import os
from shipyard_blueprints import DropboxClient

def main():
    key = os.getenv('DROPBOX_ACCESS_TOKEN')
    dropbox = DropboxClient(key)
    try:
        dropbox.connect()
        dropbox.logger.info("Successfully connected to Dropbox")
        sys.exit(0)
    except Exception as e:
        dropbox.logger.error(
            "Could not connect to Dropbox with the access key provided"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
