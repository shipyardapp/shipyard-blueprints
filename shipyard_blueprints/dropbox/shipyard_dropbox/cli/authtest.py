import sys
import os
from shipyard_dropbox import DropboxClient
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def main():
    try:
        sys.exit(DropboxClient(os.getenv("DROPBOX_ACCESS_TOKEN")).connect())
    except Exception as e:
        logger.authtest(
            f"Failed to authenticate using key. Message from Dropbox Server: {e}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()